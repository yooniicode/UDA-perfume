import time
import traceback
import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    ElementClickInterceptedException,
    WebDriverException,
)
import csv
import os
import sys
from tenacity import retry, stop_after_attempt, wait_exponential
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from queue import Queue
import random  # 랜덤 딜레이 및 UA 선택용

# -----------------------
# 1. 기본 설정 / 로그
# -----------------------

if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except (AttributeError, OSError):
        pass

logging.getLogger('selenium').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('undetected_chromedriver').setLevel(logging.WARNING)
os.environ['WDM_LOG'] = '0'

# -----------------------
# 2. 크롤러 설정
# -----------------------

# --- 2.1. 기본 설정 ---
SEARCH_KEYWORD = "chloe"
PERFUME_CSV_FILE = f'fragrantica_perfumes_{SEARCH_KEYWORD.lower().replace(" ", "-")}.csv'
REVIEW_CSV_FILE = f'fragrantica_reviews_{SEARCH_KEYWORD.lower().replace(" ", "-")}.csv'

# [수정] 고정 딜레이 대신 랜덤 딜레이 범위 사용 (3초 ~ 7초 사이)
RATE_LIMIT_DELAY_RANGE = (3.0, 7.0)
MAX_WORKERS = 3

# [추가] User-Agent 리스트 (브라우저 위장)
USER_AGENT_LIST = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0'
]

# --- 2.2. CSV 파일 헤더 ---
PERFUME_FIELDNAMES = [
    'url', 'product_name', 'brand_name', 'target_gender', 'image_url',
    'top_notes', 'middle_notes', 'base_notes',
]
REVIEW_FIELDNAMES = [
    'product_name', 'review_content', 'review_date', 'reviewer_name'
]

# --- 2.3. 선택자 (Selectors) ---
PRIMARY_PRODUCT_LINK_SELECTOR = (By.CSS_SELECTOR, "a.prefumeHbox")
FALLBACK_PRODUCT_LINK_SELECTOR = (By.CSS_SELECTOR, "a.perfumeHbox")
ALTERNATIVE_PRODUCT_LINK_SELECTOR = (By.CSS_SELECTOR, "div.perfume-card > a")

# [제품 정보]
PRODUCT_NAME_H1_SELECTOR = (By.CSS_SELECTOR, 'h1[itemprop="name"]')
BRAND_NAME_SELECTOR = (By.CSS_SELECTOR, 'span[itemprop="brand"] a span')
TARGET_GENDER_SELECTOR = (By.CSS_SELECTOR, 'h1[itemprop="name"] small')
IMAGE_URL_SELECTOR = (By.CSS_SELECTOR, 'img[itemprop="image"]')

# [리뷰 정보]
REVIEW_HOLDER_SELECTOR = (By.ID, "all-reviews")
REVIEW_BODY_SELECTOR = (By.CSS_SELECTOR, "div[itemprop='reviewBody']")
REVIEW_CONTAINER_SELECTOR = (By.CSS_SELECTOR, 'div.fragrance-review-box[itemprop="review"]')
REVIEW_CONTENT_SELECTOR = (By.CSS_SELECTOR, 'div[itemprop="reviewBody"] p')
REVIEW_DATE_SELECTOR = (By.CSS_SELECTOR, 'span[itemprop="datePublished"]')
REVIEWER_NAME_SELECTOR = (By.CSS_SELECTOR, 'p > b > a[href*="member"]')

# --- 2.4. 스레드 락 (Locks) ---
csv_lock = threading.Lock()
print_lock = threading.Lock()


# -----------------------
# 3. 드라이버 풀 클래스
# -----------------------

class DriverPool:
    """드라이버를 미리 생성하고 재사용하는 풀"""

    def __init__(self, size=3):
        self.pool = Queue(maxsize=size)
        self.size = size
        safe_print(f"\n🔧 드라이버 풀 초기화 중 ({size}개)...")
        for i in range(size):
            try:
                # [수정] 각 드라이버에 랜덤 User-Agent 할당
                user_agent = random.choice(USER_AGENT_LIST)
                driver = self._create_driver(user_agent=user_agent)
                self.pool.put(driver)
                safe_print(f"   ✅ 드라이버 {i + 1}/{size} 생성 완료 (UA: {user_agent[:40]}...)")
                time.sleep(1)
            except Exception as e:
                safe_print(f"   ❌ 드라이버 {i + 1} 생성 실패: {repr(e)}")
        safe_print(f"✅ 드라이버 풀 준비 완료\n")

    def _create_driver(self, user_agent=None):  # [수정] user_agent 인수 추가
        """단일 드라이버 생성"""
        options = uc.ChromeOptions()
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-extensions')
        options.add_argument('--log-level=3')

        # [수정] 기본 UA 대신 선택된 랜덤 UA 적용
        if user_agent:
            options.add_argument(f'--user-agent={user_agent}')
        else:
            # 기본 UA (폴백)
            options.add_argument(
                '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/120.0.0.0 Safari/537.36'
            )

        driver = uc.Chrome(options=options, use_subprocess=False)
        driver.implicitly_wait(3)
        return driver

    def get(self):
        return self.pool.get()

    def put(self, driver):
        self.pool.put(driver)

    def close_all(self):
        while not self.pool.empty():
            try:
                driver = self.pool.get_nowait()
                driver.quit()
            except:
                pass


# -----------------------
# 4. 헬퍼 함수
# -----------------------

def setup_csv_files():
    """CSV 파일이 없으면 헤더와 함께 생성."""
    try:
        if not os.path.exists(PERFUME_CSV_FILE):
            with open(PERFUME_CSV_FILE, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=PERFUME_FIELDNAMES)
                writer.writeheader()
        if not os.path.exists(REVIEW_CSV_FILE):
            with open(REVIEW_CSV_FILE, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=REVIEW_FIELDNAMES)
                writer.writeheader()
    except PermissionError as e:
        print("\n" + "!" * 60)
        print(f"❌ [치명적 오류] 파일 접근 권한이 없습니다: {e}")
        print(f"   '{PERFUME_CSV_FILE}' 또는 '{REVIEW_CSV_FILE}' 파일이")
        print("   Excel 등 다른 프로그램에서 열려 있는지 확인하고 모두 닫은 후 다시 시도하세요.")
        print("!" * 60 + "\n")
        sys.exit(1)
    except Exception as e:
        print(f"❌ CSV 파일 설정 중 알 수 없는 오류 발생: {e}")
        sys.exit(1)


def click_with_js(driver, element):
    try:
        driver.execute_script("arguments[0].click();", element)
    except Exception:
        pass


def safe_find_text(driver_or_element, *selector, wait_time=2, default=""):
    try:
        element = WebDriverWait(driver_or_element, wait_time).until(
            EC.presence_of_element_located(selector)
        )
        return element.text.strip()
    except (NoSuchElementException, TimeoutException):
        return default


def safe_find_attr(driver_or_element, *selector, attr="src", wait_time=2, default=""):
    try:
        element = WebDriverWait(driver_or_element, wait_time).until(
            EC.presence_of_element_located(selector)
        )
        return element.get_attribute(attr)
    except (NoSuchElementException, TimeoutException):
        return default


def get_notes_by_type(driver, note_type):
    """ 'Top Notes', 'Middle Notes', 'Base Notes' 헤더로 노트를 찾습니다. """
    notes = []
    try:
        xpath = f"//h4[b='{note_type} Notes']/following-sibling::div[1]//div[contains(@style, 'margin')]/div[last()]"
        note_elements = driver.find_elements(By.XPATH, xpath)
        notes = [elem.text.strip() for elem in note_elements if elem.text.strip()]
    except Exception:
        pass
    return ", ".join(notes) if notes else ""


# ======================================================================

def get_undivided_notes(driver):
    """ 'Fragrance Notes' (통합) 헤더로 노트를 찾습니다. """
    notes = []
    try:
        xpath = (
            "//span[contains(., 'Fragrance Notes')]/following::div"
            "[contains(@style, 'flex-flow: wrap') or contains(@style, 'flex-wrap: wrap')][1]"
            "/.//div[contains(@style, 'margin')]/div[last()]"
        )

        note_elements = driver.find_elements(By.XPATH, xpath)

        if not note_elements:
            xpath_h4 = (
                "//h4[b='Fragrance Notes']/following-sibling::div[1]"
                "/.//div[contains(@style, 'margin')]/div[last()]"
            )
            note_elements = driver.find_elements(By.XPATH, xpath_h4)

        notes = [elem.text.strip() for elem in note_elements if elem.text.strip()]
    except Exception:
        pass

    return ", ".join(notes) if notes else ""


class RateLimitError(Exception):
    """429 Too Many Requests 의심 시 사용"""
    pass


def is_rate_limited_page(driver):
    """
    Cloudflare 429/차단 페이지 추정:
    - 'Too Many Requests' 같은 문구
    - Cloudflare 에러 페이지 구조 등
    """
    try:
        html = driver.page_source.lower()
    except Exception:
        return False

    keywords = [
        "too many requests",
        "rate limited",
        "attention required",   # cloudflare challenge 페이지 제목
        "error 429",
    ]
    return any(k in html for k in keywords)


# ======================================================================

def write_batch_to_csv(filename, fieldnames, data_batch):
    if not data_batch:
        return
    with csv_lock:
        with open(filename, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writerows(data_batch)


def safe_print(message):
    with print_lock:
        print(message)


# -----------------------
# 5. URL 수집 함수
# -----------------------

def collect_all_product_urls(start_url, max_same_rounds=8, wait_between_scrolls=4.0):
    """Designers 페이지에서 모든 제품 URL 수집"""
    safe_print(f"🚀 [1단계] '{start_url}'에서 URL 수집 시작...")
    options = uc.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--start-maximized')
    options.add_argument(f'--user-agent={random.choice(USER_AGENT_LIST)}')

    driver = uc.Chrome(options=options, use_subprocess=False)
    wait = WebDriverWait(driver, 20)

    all_product_urls_set = set()

    selectors_to_try = [
        PRIMARY_PRODUCT_LINK_SELECTOR,
        FALLBACK_PRODUCT_LINK_SELECTOR,
        ALTERNATIVE_PRODUCT_LINK_SELECTOR
    ]
    selector_in_use = None

    try:
        driver.get(start_url)
        time.sleep(3)  # 초기 로딩 대기
        safe_print(f"✅ '{start_url}' 접속 완료")

        # 🔧 선택자 찾기
        for i, selector in enumerate(selectors_to_try):
            try:
                wait.until(EC.presence_of_element_located(selector))
                selector_in_use = selector
                safe_print(f"🔎 선택자 #{i + 1} 로 제품 요소 확인됨.")
                break
            except TimeoutException:
                safe_print(f"⚠️ 선택자 #{i + 1} 없음. 다음 시도...")

        if not selector_in_use:
            safe_print("❌ 모든 선택자로 요소를 찾지 못함. selector를 다시 확인하세요.")
            return []

        # 🔧 페이지네이션 확인
        pagination_links = driver.find_elements(By.CSS_SELECTOR, 'div.pagination a')

        if not pagination_links:
            # --- 무한 스크롤 방식 ---
            safe_print("   (i) '무한 스크롤' 방식으로 수집합니다")
            prev_count = 0
            same_rounds = 0
            last_height = driver.execute_script("return document.body.scrollHeight")
            scroll_attempt = 0

            while True:
                scroll_attempt += 1
                safe_print(f"   🔄 스크롤 시도 #{scroll_attempt}")

                try:
                    elements = driver.find_elements(*selector_in_use)
                    if not elements and prev_count == 0:
                        try:
                            wait.until(EC.presence_of_element_located(selector_in_use))
                            elements = driver.find_elements(*selector_in_use)
                        except TimeoutException:
                            safe_print("... 아직 제품 요소가 없음 (잠시 후 재시도)")

                    page_urls = [e.get_attribute('href') for e in elements if e.get_attribute('href')]
                    newly_found = set(page_urls) - all_product_urls_set
                    if newly_found:
                        all_product_urls_set.update(newly_found)
                        safe_print(f"➕ 새 URL {len(newly_found)}개 발견 (누적: {len(all_product_urls_set)})")
                        same_rounds = 0

                    # 스크롤 방식 개선
                    if elements:
                        driver.execute_script(
                            "arguments[0].scrollIntoView({behavior:'smooth', block:'end'});",
                            elements[-1]
                        )
                        time.sleep(1)
                        driver.execute_script("window.scrollBy(0, 500);")
                    else:
                        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

                    time.sleep(wait_between_scrolls)

                    # 요소 증가 대기
                    try:
                        WebDriverWait(driver, 10).until(
                            lambda d: len(d.find_elements(*selector_in_use)) > prev_count
                        )
                        prev_count = len(driver.find_elements(*selector_in_use))
                        same_rounds = 0
                        safe_print("🔄 요소 수 증가 확인 — 계속 수집")
                    except TimeoutException:
                        same_rounds += 1
                        safe_print(f"⏱ 변화 없음 (연속 {same_rounds}/{max_same_rounds})")

                    new_height = driver.execute_script("return document.body.scrollHeight")
                    if new_height == last_height:
                        same_rounds += 1
                        safe_print(f"📏 페이지 높이 변화 없음 (연속: {same_rounds})")
                    else:
                        last_height = new_height
                        same_rounds = 0

                    if same_rounds >= max_same_rounds:
                        safe_print("🏁 더 이상의 콘텐츠 로드 없음. 수집 종료.")
                        break

                    if scroll_attempt > 100:
                        safe_print("⚠️ 최대 스크롤 시도 횟수 도달. 종료.")
                        break

                except Exception as e:
                    safe_print(f"⚠️ 무한 스크롤 중 예외: {repr(e)}")
                    break
        else:
            # --- 페이지네이션 방식 ---
            safe_print("   (i) '페이지네이션' 방식으로 수집합니다")
            page_num = 1
            while True:
                try:
                    wait.until(EC.presence_of_element_located(selector_in_use))
                    elements = driver.find_elements(*selector_in_use)

                    page_urls = []
                    for elem in elements:
                        href = elem.get_attribute('href')
                        if href and href.startswith("https://www.fragrantica.com/perfume/"):
                            page_urls.append(href)

                    new_urls_count = len(set(page_urls) - all_product_urls_set)
                    all_product_urls_set.update(page_urls)
                    safe_print(f"📄 페이지 {page_num}: {new_urls_count}개 신규 수집 (누적: {len(all_product_urls_set)}개)")

                except TimeoutException:
                    safe_print(f"⚠️  페이지 {page_num}에서 제품 링크를 찾을 수 없음")

                try:
                    next_button = wait.until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, 'a[aria-label="Next »"]'))
                    )
                    click_with_js(driver, next_button)
                    time.sleep(3)
                    page_num += 1
                except (TimeoutException, NoSuchElementException):
                    safe_print("🏁 더 이상 '다음' 페이지가 없습니다. URL 수집 종료.")
                    break

    except Exception as e:
        safe_print(f"❌ URL 수집 중 치명적 오류: {repr(e)}")
        traceback.print_exc()
    finally:
        safe_print("====== 🔧 URL 수집 드라이버 종료 ======")
        driver.quit()

    return list(all_product_urls_set)

# -----------------------
# 6. 핵심 스크래핑 함수
# -----------------------

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def scrape_product_details(driver, url):
    """
    제품 상세 페이지에서 향수 정보를 스크랩.
    """
    wait = WebDriverWait(driver, 10)

    h1_element = wait.until(
        EC.presence_of_element_located(PRODUCT_NAME_H1_SELECTOR)
    )

    product_name = driver.execute_script(
        "return arguments[0].firstChild.textContent.trim()", h1_element
    )
    brand_name = safe_find_text(h1_element, *BRAND_NAME_SELECTOR, default=SEARCH_KEYWORD.title())
    target_gender = safe_find_text(h1_element, *TARGET_GENDER_SELECTOR, default="NA")

    image_url = safe_find_attr(driver, *IMAGE_URL_SELECTOR, attr="src", default="")

    # --- 노트 수집 ---
    top_notes = get_notes_by_type(driver, "Top")
    middle_notes = get_notes_by_type(driver, "Middle")
    base_notes = get_notes_by_type(driver, "Base")

    if not top_notes and not middle_notes and not base_notes:
        safe_print(f"      ... {product_name}: T/M/B 노트 없음. 'Fragrance Notes' 통합 검색 시도...")
        undivided_notes = get_undivided_notes(driver)
        if undivided_notes:
            middle_notes = undivided_notes
            safe_print(f"      ... {product_name}: 통합 노트 발견. Middle에 저장.")

    product_data = {
        'url': url,
        'product_name': product_name,
        'brand_name': brand_name,
        'target_gender': target_gender,
        'image_url': image_url,
        'top_notes': top_notes,
        'middle_notes': middle_notes,
        'base_notes': base_notes,
    }

    return product_name, product_data


def scrape_reviews(driver, product_name, base_url):
    """
    [15차 최종] #all-reviews 앵커 링크로 직접 이동
    """
    reviews_batch = []
    processed_review_identifiers = set()

    try:
        # 🔧 STEP 1: 리뷰 섹션으로 직접 이동
        review_url = base_url + "#all-reviews"
        safe_print(f"      ... {product_name}: 리뷰 섹션으로 이동 ({review_url})")

        # 429 / 차단 페이지 감지용 재시도 루프
        max_attempts = 30
        for attempt in range(1, max_attempts + 1):
            driver.get(review_url)
            time.sleep(4)  # 기본 로딩 대기

            if not is_rate_limited_page(driver):
                # 정상 페이지면 바로 진행
                break

            # 여기까지 왔다 = rate limit 의심
            wait_sec = random.randint(60, 180)  # 1~3분 랜덤 대기
            safe_print(
                f"      ⏱ {product_name}: 리뷰 요청이 rate limit에 걸린 것 같아요 "
                f"({attempt}/{max_attempts}) → {wait_sec}초 대기 후 재시도"
            )
            time.sleep(wait_sec)
        else:
            # for-else: 3번 모두 rate-limited였다면 리뷰는 포기하고 넘어감
            safe_print(f"      ❌ {product_name}: 3번 시도했지만 리뷰 페이지가 열리지 않아, 리뷰는 건너뜁니다.")
            return []

        # 🔧 STEP 2: 리뷰 섹션 존재 확인
        section_exists = driver.execute_script("""
            var section = document.getElementById('all-reviews');
            return section !== null;
        """)

        if not section_exists:
            safe_print(f"      ℹ️  {product_name}: 리뷰 섹션 없음 -> 리뷰 0개")
            return []

        safe_print(f"      ✅ {product_name}: 리뷰 섹션 발견!")
        time.sleep(2)

        # 🔧 STEP 3: 리뷰 컨테이너 확인
        review_count = driver.execute_script("""
            return document.querySelectorAll('div.fragrance-review-box[itemprop="review"]').length;
        """)

        safe_print(f"      ... {product_name}: {review_count}개 리뷰 컨테이너 감지됨")

        if review_count == 0:
            safe_print(f"      ℹ️  {product_name}: 리뷰 없음 -> 리뷰 0개")
            return []

        # 🔧 STEP 4: 무한 스크롤로 모든 리뷰 로드
        safe_print(f"      ... {product_name}: 모든 리뷰 로딩 중...")
        previous_count = 0
        no_change_count = 0
        max_no_change = 5

        while no_change_count < max_no_change:
            current_count = driver.execute_script("""
                var reviews = document.querySelectorAll('div.fragrance-review-box[itemprop="review"]');
                if (reviews.length > 0) {
                    reviews[reviews.length - 1].scrollIntoView({block: 'end', behavior: 'smooth'});
                }
                return reviews.length;
            """)

            if current_count > previous_count:
                safe_print(f"      📝 {product_name}: {current_count}개 리뷰 로드됨...")
                previous_count = current_count
                no_change_count = 0
                time.sleep(3)
            else:
                no_change_count += 1
                safe_print(f"      ⏱ {product_name}: 변화 없음 ({no_change_count}/{max_no_change})")
                time.sleep(2)

        safe_print(f"      ✅ {product_name}: 총 {previous_count}개 리뷰 로드 완료")
        time.sleep(2)

        # 🔧 STEP 5: 모든 리뷰 추출
        review_elements = driver.find_elements(By.CSS_SELECTOR, 'div.fragrance-review-box[itemprop="review"]')
        safe_print(f"      ... {product_name}: {len(review_elements)}개 리뷰 추출 시작...")

        for idx, review in enumerate(review_elements, 1):
            try:
                # 리뷰어 이름
                reviewer_name_text = "Guest"
                try:
                    meta_name = review.find_element(By.CSS_SELECTOR, 'meta[itemprop="name"]')
                    reviewer_name_text = meta_name.get_attribute("content")
                except:
                    pass

                # 날짜
                review_date_text = "NA"
                try:
                    date_span = review.find_element(By.CSS_SELECTOR, 'span[itemprop="datePublished"]')
                    review_date_text = date_span.text.strip()
                except:
                    pass

                # 리뷰 내용
                content = ""
                try:
                    content_div = review.find_element(By.CSS_SELECTOR, 'div[itemprop="reviewBody"]')
                    paragraphs = content_div.find_elements(By.TAG_NAME, 'p')
                    content = " ".join([p.text.strip() for p in paragraphs if p.text.strip()])
                except:
                    pass

                # 중복 체크
                unique_id = (reviewer_name_text, review_date_text, content[:50])

                if unique_id in processed_review_identifiers:
                    continue

                processed_review_identifiers.add(unique_id)

                if content:
                    reviews_batch.append({
                        'product_name': product_name,
                        'review_content': content,
                        'review_date': review_date_text,
                        'reviewer_name': reviewer_name_text,
                    })

                    if idx % 20 == 0:
                        safe_print(f"      ... {product_name}: {len(reviews_batch)}개 처리 중...")

            except Exception as e:
                continue

        safe_print(f"      ✅ {product_name}: 총 {len(reviews_batch)}개 리뷰 수집 완료")
        return reviews_batch

    except Exception as e:
        safe_print(f"      ❌ {product_name}: 리뷰 수집 에러: {repr(e)}")
        traceback.print_exc()
        return []

# -----------------------
# 7. 워커 함수
# -----------------------

def process_single_product(args, driver_pool):
    """단일 제품 처리 (드라이버 풀 사용)."""
    url, index, total = args
    driver = None
    product_name = url.split('/')[-1]

    try:
        driver = driver_pool.get()

        # 1️⃣ 제품 페이지 접속 및 정보 수집
        driver.get(url)
        product_name, product_data = scrape_product_details(driver, url)
        write_batch_to_csv(PERFUME_CSV_FILE, PERFUME_FIELDNAMES, [product_data])

        # 2️⃣ 페이지 전체 스크롤 (Lazy Loading 트리거)
        safe_print(f"      ... {product_name}: 페이지 전체 스크롤 중...")
        last_height = driver.execute_script("return document.body.scrollHeight")
        scroll_position = 0
        scroll_step = 800

        while scroll_position < last_height:
            scroll_position += scroll_step
            driver.execute_script(f"window.scrollTo(0, {scroll_position});")
            time.sleep(1)

            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height > last_height:
                last_height = new_height

        safe_print(f"      ✅ {product_name}: 페이지 전체 스크롤 완료")
        time.sleep(2)

        # 3️⃣ 리뷰 수집 (#all-reviews로 재접속)
        reviews_batch = scrape_reviews(driver, product_name, url)
        if reviews_batch:
            write_batch_to_csv(REVIEW_CSV_FILE, REVIEW_FIELDNAMES, reviews_batch)

        # 딜레이
        delay = random.uniform(*RATE_LIMIT_DELAY_RANGE)
        safe_print(f"      ... 다음 작업까지 {delay:.1f}초 대기 ...")
        time.sleep(delay)

        driver_pool.put(driver)

        return {
            'status': 'success',
            'product_name': product_name,
            'review_count': len(reviews_batch),
            'index': index,
            'total': total
        }

    except Exception as e:
        if driver:
            safe_print(f"  (i) {product_name} 처리 중 오류 발생. 드라이버 재시작...")
            try:
                driver.quit()
            except:
                pass

            try:
                new_user_agent = random.choice(USER_AGENT_LIST)
                new_driver = driver_pool._create_driver(user_agent=new_user_agent)
                driver_pool.put(new_driver)
                safe_print(f"  (i) 새 드라이버 생성 후 풀에 반환 완료.")
            except Exception as e_create:
                safe_print(f"  (E) 새 드라이버 생성 실패: {e_create}.")
                pass

        return {
            'status': 'failed',
            'error': repr(e)[:120],
            'url': url,
            'index': index,
            'total': total
        }

# -----------------------
# 8. 메인 실행
# -----------------------

def main():
    """메인 실행 함수 (드라이버 풀 사용)."""
    start_time = time.time()

    print("=" * 60)
    print(f"🚀 Fragrantica 크롤러 시작 (키워드: {SEARCH_KEYWORD})")
    print(f"   (드라이버 풀: {MAX_WORKERS}개)")
    print("=" * 60)

    setup_csv_files()

    formatted_keyword = SEARCH_KEYWORD.title()
    formatted_keyword = formatted_keyword.replace(" ", "-")
    start_url = f"https://www.fragrantica.com/designers/{formatted_keyword}.html"

    url_collection_start = time.time()
    product_urls = collect_all_product_urls(start_url)
    url_collection_time = time.time() - url_collection_start

    if not product_urls:
        print(f"❌ '{SEARCH_KEYWORD}'(변환: {formatted_keyword})에 대한 URL이 수집되지 않았습니다. 종료합니다.")
        return

    print(f"✅ 총 {len(product_urls)}개 제품 발견 (소요 시간: {url_collection_time:.1f}초)")

    avg_delay = sum(RATE_LIMIT_DELAY_RANGE) / 2
    avg_time_per_product = 8 + avg_delay
    estimated_time_parallel = (len(product_urls) * avg_time_per_product) / MAX_WORKERS
    print(f"\n📊 예상 소요 시간 ({MAX_WORKERS}개 병렬, 평균 딜레이 {avg_delay:.1f}초 포함): 약 {estimated_time_parallel / 60:.1f}분")

    driver_pool = DriverPool(size=MAX_WORKERS)

    print("\n[2단계] 제품 스크래핑 시작 (드라이버 풀 사용)...")
    print("-" * 60)

    scraping_start = time.time()
    total = len(product_urls)
    tasks = [(url, i + 1, total) for i, url in enumerate(product_urls)]

    success_count = 0
    failed_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_single_product, task, driver_pool): task
            for task in tasks
        }

        for future in as_completed(futures):
            result = future.result()
            percentage = (result['index'] / result['total']) * 100

            if result['status'] == 'success':
                success_count += 1
                if result['review_count'] > 0:
                    safe_print(
                        f"[{result['index']}/{result['total']} ({percentage:.1f}%)] ✅ {result['product_name']} - 리뷰 {result['review_count']}개")
                else:
                    safe_print(
                        f"[{result['index']}/{result['total']} ({percentage:.1f}%)] ✅ {result['product_name']} - 제품 정보만")
            else:
                failed_count += 1
                safe_print(
                    f"[{result['index']}/{result['total']} ({percentage:.1f}%)] ❌ 처리 실패 - {result['url']} - {result['error']}")

    print("\n🔧 드라이버 풀 종료 중...")
    driver_pool.close_all()

    scraping_time = time.time() - scraping_start
    total_time = time.time() - start_time

    print("-" * 60)
    print("\n" + "=" * 60)
    print("✅ 모든 크롤링 완료!")
    print("=" * 60)
    print(f"\n📊 통계:")
    print(f"   - 성공: {success_count}개")
    print(f"   - 실패: {failed_count}개")
    print(f"\n⏱️  소요 시간:")
    print(f"   - URL 수집: {url_collection_time:.1f}초")
    print(f"   - 제품 스크래핑: {scraping_time / 60:.1f}분")
    print(f"   - 전체: {total_time / 60:.1f}분")
    print(f"\n📁 저장된 파일:")
    print(f"   - {PERFUME_CSV_FILE}")
    print(f"   - {REVIEW_CSV_FILE}")
    print("=" * 60)


if __name__ == "__main__":
    main()