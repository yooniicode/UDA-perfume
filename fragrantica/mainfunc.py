import time
import traceback  # 상세 오류 출력을 위해 임포트
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
SEARCH_KEYWORD = "lush"
PERFUME_CSV_FILE = f'fragrantica_perfumes_{SEARCH_KEYWORD.lower().replace(" ", "-")}.csv'
REVIEW_CSV_FILE = f'fragrantica_reviews_{SEARCH_KEYWORD.lower().replace(" ", "-")}.csv'

# [수정] 딜레이 및 워커 설정 (봇 탐지 회피용)
RATE_LIMIT_DELAY_RANGE = (10.0, 20.0)  # 10초 ~ 20초 사이 랜덤 대기
MAX_WORKERS = 1  # ★★★ 반드시 1로 유지 ★★★

USER_AGENT_LIST = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.t (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
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

def collect_all_product_urls(start_url, max_same_rounds=3, wait_between_scrolls=2.0):
    """Designers 페이지에서 모든 제품 URL 수집"""
    safe_print(f"🚀 [1단계] '{start_url}'에서 URL 수집 시작...")
    options = uc.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--start-maximized')
    options.add_argument(f'--user-agent={random.choice(USER_AGENT_LIST)}')

    driver = uc.Chrome(options=options, use_subprocess=False)
    wait = WebDriverWait(driver, 15)

    all_product_urls_set = set()

    selectors_to_try = [
        PRIMARY_PRODUCT_LINK_SELECTOR,
        FALLBACK_PRODUCT_LINK_SELECTOR,
        ALTERNATIVE_PRODUCT_LINK_SELECTOR
    ]
    selector_in_use = None

    try:
        driver.get(start_url)
        safe_print(f"✅ '{start_url}' 접속 완료")

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

        pagination_links = driver.find_elements(By.CSS_SELECTOR, 'div.pagination a')

        if not pagination_links:
            # --- Dior 방식 (무한 스크롤) ---
            safe_print("   (i) '무한 스크롤' 방식으로 수집합니다")
            prev_count = 0
            same_rounds = 0
            last_height = driver.execute_script("return document.body.scrollHeight")
            while True:
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

                    if elements:
                        driver.execute_script("arguments[0].scrollIntoView({behavior:'smooth', block:'end'});",
                                              elements[-1])
                    else:
                        driver.execute_script("window.scrollBy(0, window.innerHeight);")

                    time.sleep(wait_between_scrolls)

                    try:
                        WebDriverWait(driver, 6).until(
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
                        safe_print(f"📏 페이지 높이 변화 없음 (연속 증가 체크: {same_rounds})")
                    else:
                        last_height = new_height
                        same_rounds = 0

                    if same_rounds >= max_same_rounds:
                        safe_print("🏁 더 이상의 콘텐츠 로드 없음으로 판단. 수집 종료.")
                        break
                except Exception as e:
                    safe_print(f"⚠️ 무한 스크롤 중 예외: {repr(e)}")
                    break
        else:
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
                    time.sleep(1.5)
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
    [수정] T/M/B 노트가 없는 경우, 'Fragrance Notes' (통합)를 middle_notes로 저장
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


    # 1. 표준 T/M/B 노트를 먼저 시도
    top_notes = get_notes_by_type(driver, "Top")
    middle_notes = get_notes_by_type(driver, "Middle")
    base_notes = get_notes_by_type(driver, "Base")

    # 2. 만약 T/M/B가 모두 비어있다면, 'Fragrance Notes' (통합) 케이스를 시도
    if not top_notes and not middle_notes and not base_notes:
        safe_print(f"      ... {product_name}: T/M/B 노트 없음. 'Fragrance Notes' 통합 검색 시도...")
        # [신규] 헬퍼 함수 호출
        undivided_notes = get_undivided_notes(driver)

        if undivided_notes:
            # 요청대로 undivided_notes를 middle_notes에 할당
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


def scrape_reviews(driver, product_name):
    """
    [7차 수정] 'all-reviews' 섹션 감지 후, 리뷰 '컨테이너'가 로드될 때까지 대기
    """
    reviews_batch = []
    processed_review_identifiers = set()

    try:
        # 1. 'all-reviews' 섹션이 나타날 때까지 (최대 12번) 스크롤
        safe_print(f"      ... {product_name}: 'all-reviews' 섹션이 나타날 때까지 스크롤...")
        reviews_section = None
        max_scroll_attempts = 12

        for attempt in range(max_scroll_attempts):
            try:
                # 0.5초의 짧은 대기 시간으로 'all-reviews' 요소를 찾아봄
                reviews_section = WebDriverWait(driver, 0.5).until(
                    EC.presence_of_element_located(REVIEW_HOLDER_SELECTOR)
                )
                safe_print(f"      ... {product_name}: 스크롤 {attempt + 1}회 만에 섹션 발견!")
                break  # 찾았으면 루프 탈출
            except TimeoutException:
                # 못 찾았으면 한 화면 아래로 스크롤
                driver.execute_script("window.scrollBy(0, window.innerHeight * 0.9);")
                time.sleep(0.7)  # JS가 반응할 시간

        # 2. 12번 스크롤 후에도 못 찾았으면 리뷰 0개로 처리
        if not reviews_section:
            safe_print(f"      ℹ️  {product_name}: {max_scroll_attempts}회 스크롤 후에도 리뷰 섹션 없음 -> 리뷰 0개")
            return []

        # 3. 섹션을 찾았으니 해당 위치로 정확히 이동
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", reviews_section)
        time.sleep(1)

        # 4. 무한 스크롤 루프 시작 (첫 대기 로직을 루프 안으로 이동)
        while True:

            # 5. 첫 시도(리뷰가 0개)일 경우, 리뷰 '컨테이너'가 로드될 때까지 15초간 대기
            if not processed_review_identifiers:
                try:
                    # 'all-reviews' 섹션이 있으니, 'review-box'가 나타날 때까지 15초 대기
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located(REVIEW_CONTAINER_SELECTOR)
                    )
                    safe_print(f"      ✔ {product_name}: 리뷰 블록 감지됨! 추출 시작")
                except TimeoutException:
                    # 15초를 기다려도 'review-box'가 안 뜨면, 정말 리뷰가 0개인 것임
                    safe_print(f"      ℹ️  {product_name}: 섹션은 있으나 15초 내 리뷰 로드 안됨 (리뷰 0개).")
                    break

            count_before_batch = len(processed_review_identifiers)
            review_elements = driver.find_elements(*REVIEW_CONTAINER_SELECTOR)
            new_reviews_found_this_scroll = False

            for review in review_elements:
                try:
                    # 고유 ID 생성 및 중복 확인
                    reviewer_name_text = safe_find_text(review, *REVIEWER_NAME_SELECTOR, wait_time=0.1, default="Guest")
                    review_date_text = safe_find_text(review, *REVIEW_DATE_SELECTOR, wait_time=0.1, default="NA")
                    content_preview = safe_find_text(review, *REVIEW_CONTENT_SELECTOR, wait_time=0.1, default="")[:20]

                    unique_id = (reviewer_name_text, review_date_text, content_preview)

                    if unique_id in processed_review_identifiers:
                        continue

                    processed_review_identifiers.add(unique_id)
                    new_reviews_found_this_scroll = True

                    # 내용 추출
                    content_elements = review.find_elements(*REVIEW_CONTENT_SELECTOR)
                    content = " ".join([p.text.strip() for p in content_elements if p.text.strip()])

                    if content:
                        reviews_batch.append({
                            'product_name': product_name,
                            'review_content': content,
                            'review_date': review_date_text,
                            'reviewer_name': reviewer_name_text,
                        })
                except Exception:
                    continue

            if new_reviews_found_this_scroll or count_before_batch == 0:
                safe_print(f"      📝 {product_name}: {len(reviews_batch)}개 수집됨...")

            # 종료 조건 1: 새 리뷰 없음
            if not new_reviews_found_this_scroll and count_before_batch > 0:
                safe_print(f"      🏁 {product_name}: 더 이상 새 리뷰 없음. 종료.")
                break

            # 다음 배치를 위해 마지막 요소로 스크롤
            try:
                last_element = review_elements[-1]
                driver.execute_script("arguments[0].scrollIntoView(true);", last_element)
            except IndexError:
                # 스크롤할 요소가 없음 (첫 대기에서 0개면 이미 break됨)
                break

                # 종료 조건 2: DOM 요소 개수 변화 대기 (8초)
            try:
                current_total = len(review_elements)
                WebDriverWait(driver, 8).until(
                    lambda d: len(d.find_elements(*REVIEW_CONTAINER_SELECTOR)) > current_total
                )
            except TimeoutException:
                safe_print(f"      🏁 {product_name}: 추가 로딩 없음. 수집 완료.")
                break

    except Exception as e:
        safe_print(f"      ❌ {product_name}: 리뷰 수집 중 로직 에러: {repr(e)}")
        traceback.print_exc()  # 상세 오류 확인

    safe_print(f"      ✅ {product_name}: 총 {len(reviews_batch)}개 리뷰 수집 완료")
    return reviews_batch


# -----------------------
# 7. 워커 함수
# -----------------------

def process_single_product(args, driver_pool):
    """
    단일 제품 처리.
    ★★ 작업 시작 '전'에 휴식 로직을 먼저 수행 ★★
    """
    url, index, total = args
    driver = None
    product_name = url.split('/')[-1]

    # --- 전략적 휴식 로직 (워커 스레드가 직접 수행) ---
    # 1-based index이므로, (index - 1)이 40의 배수일 때 휴식
    # (즉, 41번째, 81번째... 작업을 시작하기 '전'에 휴식)
    break_point = 40
    sleep_time_sec = 600  # 10분

    # (index - 1)이 0보다 크고, break_point의 배수일 때
    if (index - 1) > 0 and (index - 1) % break_point == 0:
        safe_print("\n" + "=" * 60)
        safe_print(f"☕️ [전략적 휴식] {index - 1}개 처리 완료. 봇 탐지 회피를 위해 {sleep_time_sec / 60:.0f}분간 휴식합니다.")
        safe_print(f"   (현재 시간: {time.strftime('%Y-%m-%d %H:%M:%S')})")
        print("=" * 60 + "\n")

        time.sleep(sleep_time_sec)  # ★★★ 작업 스레드(워커)가 직접 휴식 ★★★

        safe_print(f"✅ 휴식 완료. 다음 작업({index}/{total})을 재개합니다...\n")

    try:
        driver = driver_pool.get()
        driver.get(url)

        product_name, product_data = scrape_product_details(driver, url)
        write_batch_to_csv(PERFUME_CSV_FILE, PERFUME_FIELDNAMES, [product_data])

        reviews_batch = scrape_reviews(driver, product_name)
        if reviews_batch:
            write_batch_to_csv(REVIEW_CSV_FILE, REVIEW_FIELDNAMES, reviews_batch)

        # 고정 딜레이 대신 랜덤 딜레이 적용
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
                # 드라이버 재생성 시에도 랜덤 UA 적용
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

def get_already_scraped_urls(csv_file):
    """
    CSV 파일을 읽어 이미 수집된 URL 목록을 반환합니다.
    """
    scraped_urls = set()
    if not os.path.exists(csv_file):
        return scraped_urls

    try:
        with open(csv_file, 'r', encoding='utf-8-sig') as f:
            # 헤더를 건너뛰고 읽기 위해 DictReader 사용
            reader = csv.DictReader(f)
            for row in reader:
                # 'url' 컬럼이 존재하고 값이 있을 경우에만 추가
                if 'url' in row and row['url']:
                    scraped_urls.add(row['url'])
    except FileNotFoundError:
        pass  # 파일이 없으면 빈 set 반환
    except Exception as e:
        safe_print(f"⚠️ 기존 CSV 파일({csv_file}) 읽기 오류: {e}")
        # 파일이 손상되었을 수 있으므로, 안전을 위해 빈 set 반환
        pass

    return scraped_urls


def main():
    """
    1. '이어가기' 로직 추가 (중복 수집 방지)
    2. '전략적 휴식' 로직을 process_single_product 함수로 이동시킴
    """
    start_time = time.time()

    print("=" * 60)
    print(f"🚀 Fragrantica 크롤러 시작 (키워드: {SEARCH_KEYWORD})")
    print(f"   (드라이버 풀: {MAX_WORKERS}개, 딜레이: {RATE_LIMIT_DELAY_RANGE[0]}~{RATE_LIMIT_DELAY_RANGE[1]}초)")
    print("=" * 60)

    # --- 1. CSV 파일 준비 ---
    setup_csv_files()

    # --- 2. '이어가기' 로직: 이미 수집한 URL 불러오기 ---
    already_scraped_urls = get_already_scraped_urls(PERFUME_CSV_FILE)
    if already_scraped_urls:
        print(f"✅ [이어가기] 기존에 수집한 {len(already_scraped_urls)}개의 URL을 확인했습니다.")

    # --- 3. URL 수집 ---
    formatted_keyword = SEARCH_KEYWORD.title().replace(" ", "-")
    start_url = f"https://www.fragrantica.com/designers/{formatted_keyword}.html"

    url_collection_start = time.time()
    all_product_urls = collect_all_product_urls(start_url)
    url_collection_time = time.time() - url_collection_start

    if not all_product_urls:
        print(f"❌ '{SEARCH_KEYWORD}'에 대한 URL이 수집되지 않았습니다. 종료합니다.")
        return

    # --- 4. '이어가기' 로직: 수집할 URL 필터링 ---
    urls_to_scrape = [url for url in all_product_urls if url not in already_scraped_urls]

    print(f"\n✅ 총 {len(all_product_urls)}개 제품 발견 (소요 시간: {url_collection_time:.1f}초)")
    print(f"   - 이미 수집된 URL: {len(already_scraped_urls)}개")
    print(f"   - ❗️ 새로 수집할 URL: {len(urls_to_scrape)}개")

    if not urls_to_scrape:
        print("\n🎉 모든 제품 수집이 이미 완료되었습니다. 프로그램을 종료합니다.")
        return

    # --- 5. 예상 시간 계산 (새로 수집할 URL 기준) ---
    avg_delay = sum(RATE_LIMIT_DELAY_RANGE) / 2
    avg_time_per_product = 8 + avg_delay

    # 휴식 시간 계산 (40개당 10분(600초) 휴식)
    total_rests = (len(urls_to_scrape) // 40)
    total_rest_time = total_rests * 600

    estimated_time_total = (len(urls_to_scrape) * avg_time_per_product) + total_rest_time

    print(f"\n📊 예상 소요 시간 (딜레이 {avg_delay:.1f}초 + 휴식 {total_rests}회 포함):")
    print(f"   약 {estimated_time_total / 60:.1f}분 (또는 {estimated_time_total / 3600:.2f} 시간)")

    # --- 6. 드라이버 풀 및 스크래핑 시작 ---
    driver_pool = DriverPool(size=MAX_WORKERS)

    print("\n[2단계] 제품 스크래핑 시작 (이어가기 모드)...")
    print("-" * 60)

    scraping_start = time.time()
    total = len(urls_to_scrape)
    tasks = [(url, i + 1, total) for i, url in enumerate(urls_to_scrape)]

    success_count = 0
    failed_count = 0

    # 휴식 카운터가 더 이상 필요 없으므로 삭제
    # tasks_since_last_break = 0

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
                # tasks_since_last_break += 1 # <-- 삭제

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
    print(f"   - 총 {len(urls_to_scrape)}개 중 {success_count}개 성공")
    print(f"   - 실패: {failed_count}개")
    print(f"\n⏱️  소요 시간:")
    print(f"   - URL 수집: {url_collection_time:.1f}초")
    print(f"   - 제품 스크래핑 (휴식 시간 포함): {scraping_time / 60:.1f}분")
    print(f"   - 전체: {total_time / 60:.1f}분")
    print(f"\n📁 저장된 파일:")
    print(f"   - {PERFUME_CSV_FILE}")
    print(f"   - {REVIEW_CSV_FILE}")
    print("=" * 60)


if __name__ == "__main__":
    main()