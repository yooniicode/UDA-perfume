import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    ElementClickInterceptedException,
    WebDriverException,
)
import time
import csv
import os
import sys
from tenacity import retry, stop_after_attempt, wait_exponential
import logging
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from queue import Queue

# -----------------------
# 기본 설정 / 로그
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

# --- 1. 설정 ---
SEARCH_KEYWORD = "dior"
PERFUME_CSV_FILE = f'parfumo_perfumes_{SEARCH_KEYWORD}.csv'
REVIEW_CSV_FILE = f'parfumo_reviews_{SEARCH_KEYWORD}.csv'
RATE_LIMIT_DELAY = 0.3
MAX_WORKERS = 3  # 안정성을 위해 3개로 설정

# --- 2. CSV 파일 헤더 ---
PERFUME_FIELDNAMES = [
    'product_name',
    'brand_name',
    'target_gender',
    'release_year',
    'top_notes',
    'heart_notes',
    'base_notes',
]
REVIEW_FIELDNAMES = [
    'product_name',
    'review_date',
    'reviewer_name',
    'reviewer_gender',
    'reviewer_total_reviews',
    'helpful_badge',
    'award_count',
    'review_title',
    'review_content'
]

# --- 3. 선택자 ---
PRODUCT_LINK_SELECTOR = (By.CSS_SELECTOR, 'div.name > a')
NEXT_PAGE_BUTTON_SELECTOR = (By.CSS_SELECTOR, 'a.paging_links[rel="next"]')
PRODUCT_NAME_SELECTOR = (By.CSS_SELECTOR, 'h1.p_name_h1')
BRAND_NAME_SELECTOR = (By.CSS_SELECTOR, 'h1 span[itemprop="brand"] span[itemprop="name"]')
TARGET_GENDER_SELECTOR = (By.CSS_SELECTOR, 'div.p_gender_big i')
RELEASE_YEAR_SELECTOR = (By.CSS_SELECTOR, 'h1 span.label_a')
TOP_NOTES_SELECTOR = (By.CSS_SELECTOR, 'span[data-nt="t"] span.nowrap')
HEART_NOTES_SELECTOR = (By.CSS_SELECTOR, 'span[data-nt="m"] span.nowrap')
BASE_NOTES_SELECTOR = (By.CSS_SELECTOR, 'span[data-nt="b"] span.nowrap')
REVIEW_TITLE_SELECTOR = (By.CSS_SELECTOR, 'div.text-lg.bold span[itemprop="name"]')
REVIEW_CONTENT_SELECTOR = (By.CSS_SELECTOR, 'div.leading-7')
READ_MORE_BUTTON_SELECTOR = (By.XPATH, ".//div[contains(text(), 'Read more')]")
MORE_REVIEWS_BUTTON_SELECTOR = (By.CSS_SELECTOR, 'span.action_more_reviews')
REVIEW_CONTAINER_SELECTOR = (By.CSS_SELECTOR, 'article.review')
REVIEW_DATE_SELECTOR = (By.CSS_SELECTOR, 'div[itemprop="datePublished"]')
REVIEWER_NAME_SELECTOR = (By.CSS_SELECTOR, 'span[itemprop="author"] span[itemprop="name"]')
REVIEWER_GENDER_SELECTOR = (By.CSS_SELECTOR, 'a.review_user_photo i.fa')
REVIEWER_TOTAL_REVIEWS_SELECTOR = (By.CSS_SELECTOR, 'a.review_user_photo span.text-xs')
HELPFUL_BADGE_SELECTOR = (By.CSS_SELECTOR, 'span.useful_desc_1')
AWARD_COUNT_SELECTOR = (By.CSS_SELECTOR, 'span[id^="nr_awards_"]')
MORE_REVIEWS_MAIN_BUTTON_SELECTOR = (By.CSS_SELECTOR, 'span.action_more_reviews')


# 락
csv_lock = threading.Lock()
print_lock = threading.Lock()


# -----------------------
# 4. 드라이버 풀 클래스
# -----------------------

# -----------------------
# 4. 드라이버 풀 클래스
# -----------------------

def handle_cookie_popup(driver):
    """쿠키/Privacy 팝업 처리 (재사용 가능한 함수)"""
    try:
        # iframe 방식 팝업
        iframe_element = WebDriverWait(driver, 3).until(
            EC.presence_of_element_located((By.ID, "sp_message_iframe_902160"))
        )
        driver.switch_to.frame(iframe_element)

        settings_button = WebDriverWait(driver, 3).until(
            EC.element_to_be_clickable((By.XPATH, "//button[@title='Settings or reject']"))
        )
        settings_button.click()

        save_exit_button = WebDriverWait(driver, 3).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button.sp_choice_type_SAVE_AND_EXIT"))
        )
        save_exit_button.click()

        driver.switch_to.default_content()
        time.sleep(0.5)
        return True
    except (TimeoutException, NoSuchElementException):
        driver.switch_to.default_content()

        # 일반 팝업 시도 (iframe 아닌 경우)
        try:
            accept_button = WebDriverWait(driver, 2).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Accept') or contains(text(), 'OK')]"))
            )
            accept_button.click()
            time.sleep(0.5)
            return True
        except:
            pass

    return False


class DriverPool:
    """드라이버를 미리 생성하고 재사용하는 풀"""

    def __init__(self, size=3):
        self.pool = Queue(maxsize=size)
        self.size = size
        print(f"\n🔧 드라이버 풀 초기화 중 ({size}개)...")

        for i in range(size):
            try:
                driver = self._create_driver()
                self.pool.put(driver)
                print(f"   ✅ 드라이버 {i + 1}/{size} 생성 완료 (쿠키 설정 포함)")
                time.sleep(1)  # 생성 간격
            except Exception as e:
                print(f"   ❌ 드라이버 {i + 1} 생성 실패: {repr(e)}")

        print(f"✅ 드라이버 풀 준비 완료\n")

    def _create_driver(self):
        """단일 드라이버 생성 (쿠키 사전 설정 포함)"""
        options = uc.ChromeOptions()
        # 메모리 관련 옵션 추가
        options.add_argument('--memory-pressure-off')
        options.add_argument('--disable-background-timer-throttling')
        options.add_argument('--disable-renderer-backgrounding')

        # 기존 옵션들
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-extensions')
        options.add_argument('--blink-settings=imagesEnabled=false')
        options.add_argument('--log-level=3')
        options.add_argument(
            '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/120.0.0.0 Safari/537.36'
        )

        driver = uc.Chrome(options=options, use_subprocess=False)
        driver.set_page_load_timeout(30)  # 타임아웃 추가
        driver.implicitly_wait(3)

        # 메인 페이지 방문하여 쿠키 처리
        try:
            driver.get("https://www.parfumo.com/")
            if handle_cookie_popup(driver):
                pass  # 팝업 처리 성공
            time.sleep(0.5)
        except Exception as e:
            print(f"      ⚠️ 쿠키 처리 중 오류 (계속 진행): {repr(e)[:50]}")

        return driver

    def create_driver(self):
        """public 메서드 추가 - 외부에서 새 드라이버 생성 시 사용"""
        return self._create_driver()

    def is_driver_alive(self, driver):
        """드라이버가 살아있는지 확인"""
        try:
            _ = driver.current_url
            _ = driver.window_handles
            return True
        except:
            return False

    def get(self):
        """풀에서 건강한 드라이버 가져오기"""
        driver = self.pool.get()

        # 드라이버 상태 확인
        if not self.is_driver_alive(driver):
            print(f"      ⚠️ 죽은 드라이버 감지, 새로 생성 중...")
            try:
                driver.quit()
            except:
                pass
            driver = self._create_driver()

        return driver

    def put(self, driver):
        """건강한 드라이버만 풀에 반환"""
        if self.is_driver_alive(driver):
            self.pool.put(driver)
        else:
            # 죽은 드라이버는 새로 생성해서 반환
            print(f"      ⚠️ 죽은 드라이버 대체 중...")
            try:
                driver.quit()
            except:
                pass
            new_driver = self._create_driver()
            self.pool.put(new_driver)

    def close_all(self):
        """모든 드라이버 종료"""
        while not self.pool.empty():
            try:
                driver = self.pool.get_nowait()
                driver.quit()
            except:
                pass

# -----------------------
# 5. 헬퍼 함수
# -----------------------

def setup_csv_files():
    """CSV 파일이 없으면 헤더와 함께 생성."""
    if not os.path.exists(PERFUME_CSV_FILE):
        with open(PERFUME_CSV_FILE, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=PERFUME_FIELDNAMES)
            writer.writeheader()

    if not os.path.exists(REVIEW_CSV_FILE):
        with open(REVIEW_CSV_FILE, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=REVIEW_FIELDNAMES)
            writer.writeheader()


def click_with_js(driver, element):
    """JavaScript로 클릭."""
    try:
        driver.execute_script("arguments[0].click();", element)
    except Exception:
        pass


def safe_find_text(driver_or_element, *selector, wait_time=2):
    """요소를 찾아 텍스트를 반환하되, 없으면 빈 문자열."""
    try:
        element = WebDriverWait(driver_or_element, wait_time).until(
            EC.presence_of_element_located(selector)
        )
        return element.text
    except (NoSuchElementException, TimeoutException):
        return ""


def get_notes(driver, *selector):
    """노트 요소를 모두 찾아 쉼표로 연결."""
    try:
        elements = driver.find_elements(*selector)
        notes = [elem.text for elem in elements if elem.text]
        return ", ".join(notes)
    except NoSuchElementException:
        return ""


def write_batch_to_csv(filename, fieldnames, data_batch):
    """배치 데이터를 스레드 안전하게 CSV에 쓰기."""
    if not data_batch:
        return
    with csv_lock:
        with open(filename, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writerows(data_batch)


def safe_print(message):
    """스레드 안전 출력"""
    with print_lock:
        print(message)


# -----------------------
# 6. 핵심 스크래핑 함수
# -----------------------

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def scrape_product_details(driver):
    """제품 상세 페이지에서 향수 정보를 스크랩."""
    wait = WebDriverWait(driver, 8)

    product_name_element = wait.until(
        EC.presence_of_element_located(PRODUCT_NAME_SELECTOR)
    )
    product_name = driver.execute_script(
        "return arguments[0].firstChild.textContent.trim()", product_name_element
    )

    brand_name = safe_find_text(driver, *BRAND_NAME_SELECTOR)
    release_year = safe_find_text(driver, *RELEASE_YEAR_SELECTOR)

    target_gender = "N/A"
    try:
        icon_class = driver.find_element(*TARGET_GENDER_SELECTOR).get_attribute('class')
        if 'fa-mars' in icon_class:
            target_gender = 'M'
        elif 'fa-venus' in icon_class:
            target_gender = 'F'
        elif 'fa-venus-mars' in icon_class:
            target_gender = 'N'
    except NoSuchElementException:
        pass

    top_notes = get_notes(driver, *TOP_NOTES_SELECTOR)
    heart_notes = get_notes(driver, *HEART_NOTES_SELECTOR)
    base_notes = get_notes(driver, *BASE_NOTES_SELECTOR)

    product_data = {
        'product_name': product_name,
        'brand_name': brand_name,
        'target_gender': target_gender,
        'release_year': release_year,
        'top_notes': top_notes,
        'heart_notes': heart_notes,
        'base_notes': base_notes,
    }

    return product_name, product_data


def scrape_reviews(driver, product_name):
    """제품 페이지의 모든 리뷰 스크랩."""
    processed_review_texts = set()
    reviews_batch = []

    # 리뷰 섹션 찾기 및 스크롤
    try:
        reviews_section = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "reviews_holder"))
        )
        driver.execute_script(
            "arguments[0].scrollIntoView({block: 'center'});", reviews_section
        )
        time.sleep(1)
    except Exception:
        safe_print(f"      ℹ️  {product_name}: 리뷰 섹션 없음")
        return reviews_batch

    # 초기 리뷰 개수 확인
    initial_review_count = len(driver.find_elements(*REVIEW_CONTAINER_SELECTOR))
    safe_print(f"      📝 {product_name}: 초기 리뷰 {initial_review_count}개 발견")

    # 🆕 메인 "More reviews" 버튼 클릭 루프 (페이지 하단)
    click_count = 0
    while True:
        try:
            # 현재 로드된 리뷰 개수 확인
            current_review_count = len(driver.find_elements(*REVIEW_CONTAINER_SELECTOR))

            # 메인 "More reviews" 버튼 찾기
            more_reviews_main_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable(MORE_REVIEWS_MAIN_BUTTON_SELECTOR)
            )

            # 버튼이 보이면 클릭
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", more_reviews_main_button)
            time.sleep(0.5)
            click_with_js(driver, more_reviews_main_button)
            click_count += 1

            # 새 리뷰가 로드될 때까지 대기
            WebDriverWait(driver, 10).until(
                lambda d: len(d.find_elements(*REVIEW_CONTAINER_SELECTOR)) > current_review_count
            )

            new_review_count = len(driver.find_elements(*REVIEW_CONTAINER_SELECTOR))
            safe_print(f"      🔄 {product_name}: 'More reviews' 클릭 #{click_count} - 리뷰 {new_review_count}개로 증가")
            time.sleep(1)

        except (TimeoutException, NoSuchElementException):
            # 더 이상 버튼이 없으면 종료
            if click_count > 0:
                safe_print(f"      ✅ {product_name}: 모든 리뷰 로드 완료 (총 {click_count}번 클릭)")
            break

    # 🔄 리뷰 수집 루프 (기존 로직)
    review_elements = driver.find_elements(*REVIEW_CONTAINER_SELECTOR)
    total_reviews = len(review_elements)
    safe_print(f"      📊 {product_name}: {total_reviews}개 리뷰 처리 시작...")

    for idx, review in enumerate(review_elements, 1):
        try:
            # "Read more" 버튼 펼치기
            try:
                read_more_button = review.find_element(*READ_MORE_BUTTON_SELECTOR)
                if read_more_button.is_displayed():
                    click_with_js(driver, read_more_button)
                    time.sleep(0.3)
            except NoSuchElementException:
                pass

            # 리뷰 내용 수집
            content = safe_find_text(review, *REVIEW_CONTENT_SELECTOR, wait_time=1)

            if content and content not in processed_review_texts:
                processed_review_texts.add(content)

                # 기존 정보
                title = safe_find_text(review, *REVIEW_TITLE_SELECTOR, wait_time=1)

                # 새로운 정보 수집
                review_date = safe_find_text(review, *REVIEW_DATE_SELECTOR, wait_time=1)
                reviewer_name = safe_find_text(review, *REVIEWER_NAME_SELECTOR, wait_time=1)

                # 리뷰어 성별
                reviewer_gender = "N/A"
                try:
                    gender_icon = review.find_element(*REVIEWER_GENDER_SELECTOR)
                    icon_class = gender_icon.get_attribute('class')
                    if 'fa-mars' in icon_class:
                        reviewer_gender = 'M'
                    elif 'fa-venus' in icon_class:
                        reviewer_gender = 'F'
                except NoSuchElementException:
                    pass

                # 리뷰어 총 리뷰 수
                reviewer_total_reviews = "0"
                try:
                    reviews_text = safe_find_text(review, *REVIEWER_TOTAL_REVIEWS_SELECTOR, wait_time=1)
                    import re
                    match = re.search(r'(\d+)\s+Reviews?', reviews_text)
                    if match:
                        reviewer_total_reviews = match.group(1)
                except:
                    pass

                # 유용성 배지
                helpful_badge = safe_find_text(review, *HELPFUL_BADGE_SELECTOR, wait_time=1)

                # 어워드 수
                award_count = "0"
                try:
                    award_text = safe_find_text(review, *AWARD_COUNT_SELECTOR, wait_time=1)
                    if award_text:
                        award_count = award_text.strip()
                except:
                    pass

                reviews_batch.append({
                    'product_name': product_name,
                    'review_date': review_date,
                    'reviewer_name': reviewer_name,
                    'reviewer_gender': reviewer_gender,
                    'reviewer_total_reviews': reviewer_total_reviews,
                    'helpful_badge': helpful_badge,
                    'award_count': award_count,
                    'review_title': title,
                    'review_content': content,
                })

                # 진행 상황 로그 (50개마다)
                if idx % 50 == 0:
                    safe_print(f"      ⏳ {product_name}: {idx}/{total_reviews} 리뷰 처리 중... (수집: {len(reviews_batch)}개)")

        except Exception as e:
            safe_print(f"      ⚠️  {product_name}: 리뷰 #{idx} 처리 실패 - {repr(e)[:50]}")
            continue

    safe_print(f"      ✅ {product_name}: 총 {len(reviews_batch)}개 리뷰 수집 완료")
    return reviews_batch

def find_search_bar_and_button(driver, wait, keyword: str):
    """검색창 & 버튼을 여러 방식으로 시도."""
    search_candidates = [
        (By.ID, "s_top"),
        (By.CSS_SELECTOR, "input[name='q']"),
        (By.CSS_SELECTOR, "input[type='search']"),
        (By.CSS_SELECTOR, "input[placeholder*='Perfume']"),
        (By.CSS_SELECTOR, "input[placeholder*='Search']"),
    ]

    search_bar = None
    for by_, sel_ in search_candidates:
        try:
            search_bar = wait.until(
                EC.element_to_be_clickable((by_, sel_))
            )
            break
        except TimeoutException:
            continue

    if search_bar is None:
        raise TimeoutException("검색창을 찾지 못했습니다.")

    search_bar.clear()
    search_bar.send_keys(keyword)

    button_candidates = [
        (By.CSS_SELECTOR, "button.btn-s-ext"),
        (By.CSS_SELECTOR, "button[type='submit']"),
        (By.CSS_SELECTOR, "form button"),
    ]

    clicked = False
    for by_, sel_ in button_candidates:
        try:
            btn = wait.until(
                EC.element_to_be_clickable((by_, sel_))
            )
            click_with_js(driver, btn)
            clicked = True
            break
        except (TimeoutException, ElementClickInterceptedException):
            continue

    if not clicked:
        search_bar.send_keys(u"\ue007")


# -----------------------
# 7. URL 수집 함수
# -----------------------
def collect_all_product_urls():
    """모든 검색 결과 페이지에서 제품 URL 수집."""
    options = uc.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    driver = uc.Chrome(options=options, use_subprocess=False)
    wait = WebDriverWait(driver, 15)
    all_product_urls = []

    try:
        driver.get("https://www.parfumo.com/")
        time.sleep(2)

        # 🔄 handle_cookie_popup() 함수 사용으로 변경
        if handle_cookie_popup(driver):
            print("✅ Privacy 팝업 처리 완료")
        else:
            print("ℹ️  Privacy 팝업이 없거나 이미 처리됨")

        print("🔍 검색창/버튼 찾는 중...")
        find_search_bar_and_button(driver, wait, SEARCH_KEYWORD)
        print(f"🔍 '{SEARCH_KEYWORD}' 검색 요청 전송 완료")

        page_num = 1
        while True:
            try:
                wait.until(EC.presence_of_element_located(PRODUCT_LINK_SELECTOR))
                product_link_elements = driver.find_elements(*PRODUCT_LINK_SELECTOR)
                page_urls = [
                    elem.get_attribute('href')
                    for elem in product_link_elements
                    if elem.get_attribute('href')
                ]

                all_product_urls.extend(page_urls)
                print(f"📄 페이지 {page_num}: {len(page_urls)}개 수집 (누적: {len(all_product_urls)}개)")

            except TimeoutException:
                print(f"⚠️  페이지 {page_num}에서 제품 링크를 찾을 수 없음")

            try:
                next_button = wait.until(
                    EC.presence_of_element_located(NEXT_PAGE_BUTTON_SELECTOR)
                )
                next_page_url = next_button.get_attribute('href')
                if not next_page_url:
                    break
                driver.get(next_page_url)
                time.sleep(1)
                page_num += 1
            except (TimeoutException, NoSuchElementException):
                break

    except Exception as e:
        print(f"❌ URL 수집 중 오류: {repr(e)}")
        traceback.print_exc()
    finally:
        driver.quit()

    return all_product_urls


# -----------------------
# 8. 워커 함수
# -----------------------

def process_single_product(args, driver_pool):
    """단일 제품 처리 (드라이버 풀 사용)."""
    url, index, total = args
    driver = None
    retry_count = 0
    max_retries = 3

    while retry_count < max_retries:
        try:
            # 풀에서 드라이버 가져오기
            driver = driver_pool.get()

            # 드라이버 건강 체크
            try:
                _ = driver.current_url
            except:
                # 드라이버가 죽었으면 새로 생성
                safe_print(f"      ⚠️ 드라이버 세션 종료 감지, 새 드라이버 생성 중...")
                try:
                    driver.quit()
                except:
                    pass
                driver = driver_pool._create_driver()

            driver.get(url)

            # 제품 정보 스크랩
            product_name, product_data = scrape_product_details(driver)
            write_batch_to_csv(PERFUME_CSV_FILE, PERFUME_FIELDNAMES, [product_data])

            # 리뷰 스크랩
            reviews_batch = scrape_reviews(driver, product_name)
            if reviews_batch:
                write_batch_to_csv(REVIEW_CSV_FILE, REVIEW_FIELDNAMES, reviews_batch)

            time.sleep(RATE_LIMIT_DELAY)

            # 성공 시 드라이버 풀에 반환
            driver_pool.put(driver)

            return {
                'status': 'success',
                'product_name': product_name,
                'review_count': len(reviews_batch),
                'index': index,
                'total': total
            }

        except InvalidSessionIdException as e:
            # 세션 오류 시 재시도
            retry_count += 1
            safe_print(f"      🔄 세션 오류 발생, 재시도 {retry_count}/{max_retries}")

            if driver:
                try:
                    driver.quit()
                except:
                    pass
                # 새 드라이버 생성
                driver = driver_pool._create_driver()

            if retry_count >= max_retries:
                # 최대 재시도 횟수 초과
                if driver:
                    driver_pool.put(driver)
                return {
                    'status': 'failed',
                    'error': f'InvalidSessionIdException after {max_retries} retries',
                    'url': url,
                    'index': index,
                    'total': total
                }

            time.sleep(5)  # 재시도 전 대기
            continue

        except Exception as e:
            # 다른 에러 발생 시
            if driver:
                # 드라이버가 살아있는지 확인 후 반환
                try:
                    _ = driver.current_url
                    driver_pool.put(driver)
                except:
                    # 죽은 드라이버는 새로 생성해서 반환
                    try:
                        driver.quit()
                    except:
                        pass
                    new_driver = driver_pool._create_driver()
                    driver_pool.put(new_driver)

            return {
                'status': 'failed',
                'error': repr(e)[:120],
                'url': url,
                'index': index,
                'total': total
            }

    # while 루프 종료 (여기 도달하면 안 됨)
    return {
        'status': 'failed',
        'error': 'Unexpected error',
        'url': url,
        'index': index,
        'total': total
    }


# -----------------------
# 9. 메인 실행
# -----------------------

def main():
    """메인 실행 함수 (드라이버 풀 사용)."""
    start_time = time.time()

    print("=" * 60)
    print(f"🚀 향수 크롤러 시작 (드라이버 풀: {MAX_WORKERS}개)")
    print("=" * 60)

    setup_csv_files()

    # 1단계: URL 수집
    print("\n[1단계] 제품 URL 수집 중...")
    url_collection_start = time.time()
    product_urls = collect_all_product_urls()
    url_collection_time = time.time() - url_collection_start

    if not product_urls:
        print("❌ 수집된 제품 URL이 없습니다. 종료합니다.")
        return

    print(f"✅ 총 {len(product_urls)}개 제품 발견 (소요 시간: {url_collection_time:.1f}초)")

    # 예상 시간
    avg_time_per_product = 8
    estimated_time_parallel = (len(product_urls) * avg_time_per_product) / MAX_WORKERS
    print(f"\n📊 예상 소요 시간 ({MAX_WORKERS}개 병렬): 약 {estimated_time_parallel / 60:.1f}분")

    # 드라이버 풀 생성
    driver_pool = DriverPool(size=MAX_WORKERS)

    # 2단계: 병렬 처리
    print("[2단계] 제품 스크래핑 시작 (드라이버 풀 사용)...")
    print("-" * 60)

    scraping_start = time.time()
    total = len(product_urls)

    tasks = [(url, i + 1, total) for i, url in enumerate(product_urls)]

    success_count = 0
    failed_count = 0

    # ThreadPoolExecutor로 병렬 실행
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_single_product, task, driver_pool): task
            for task in tasks
        }

        for future in as_completed(futures):
            result = future.result()

            if result['status'] == 'success':
                success_count += 1
                percentage = (result['index'] / result['total']) * 100
                if result['review_count'] > 0:
                    safe_print(
                        f"[{result['index']}/{result['total']} ({percentage:.1f}%)] ✅ {result['product_name']} - 리뷰 {result['review_count']}개")
                else:
                    safe_print(
                        f"[{result['index']}/{result['total']} ({percentage:.1f}%)] ✅ {result['product_name']} - 제품 정보만")
            else:
                failed_count += 1
                percentage = (result['index'] / result['total']) * 100
                safe_print(f"[{result['index']}/{result['total']} ({percentage:.1f}%)] ❌ 처리 실패 - {result['error']}")

    # 드라이버 풀 정리
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
    if scraping_time > 0:
        print(f"   - 속도 향상: 약 {(len(product_urls) * avg_time_per_product / 60) / (scraping_time / 60):.1f}배")
    print(f"\n📁 저장된 파일:")
    print(f"   - {PERFUME_CSV_FILE}")
    print(f"   - {REVIEW_CSV_FILE}")
    print("=" * 60)


if __name__ == "__main__":
    main()