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
import random

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
SEARCH_KEYWORD = "burberry"
PERFUME_CSV_FILE = f'fragrantica_perfumes_{SEARCH_KEYWORD.lower().replace(" ", "-")}.csv'
REVIEW_CSV_FILE = f'fragrantica_reviews_{SEARCH_KEYWORD.lower().replace(" ", "-")}.csv'

RATE_LIMIT_DELAY_RANGE = (3.0, 7.0)
MAX_WORKERS = 3

USER_AGENT_LIST = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0'
]

# --- 2.2. CSV 파일 헤더 ---
REVIEW_FIELDNAMES = [
    'product_name', 'review_content', 'review_date', 'reviewer_name'
]

# --- 2.3. 스레드 락 ---
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
                user_agent = random.choice(USER_AGENT_LIST)
                driver = self._create_driver(user_agent=user_agent)
                self.pool.put(driver)
                safe_print(f"   ✅ 드라이버 {i + 1}/{size} 생성 완료")
                time.sleep(1)
            except Exception as e:
                safe_print(f"   ❌ 드라이버 {i + 1} 생성 실패: {repr(e)}")
        safe_print(f"✅ 드라이버 풀 준비 완료\n")

    def _create_driver(self, user_agent=None):
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


def is_rate_limited_page(driver):
    """Cloudflare 429/차단 페이지 추정"""
    try:
        html = driver.page_source.lower()
    except Exception:
        return False

    keywords = [
        "too many requests",
        "rate limited",
        "attention required",
        "error 429",
    ]
    return any(k in html for k in keywords)


# -----------------------
# 5. 리뷰 수집 함수
# -----------------------

def scrape_reviews(driver, product_name, base_url):
    """
    리뷰 수집 (다중 전략)
    """
    reviews_batch = []
    processed_review_identifiers = set()

    try:
        # 🔧 STEP 1: 여러 방법으로 리뷰 섹션 찾기
        safe_print(f"      ... {product_name}: 리뷰 섹션 탐색 중...")

        # Rate limit 체크 및 재시도
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            review_url = base_url + "#all-reviews"
            driver.get(review_url)
            time.sleep(4)

            if not is_rate_limited_page(driver):
                break

            wait_sec = random.randint(60, 180)
            safe_print(
                f"      ⏱ {product_name}: Rate limit 감지 "
                f"({attempt}/{max_attempts}) → {wait_sec}초 대기"
            )
            time.sleep(wait_sec)
        else:
            safe_print(f"      ❌ {product_name}: Rate limit으로 리뷰 수집 실패")
            return []

        # 방법 1: #all-reviews 앵커로 이동
        section_exists = driver.execute_script("""
            // 여러 가능한 선택자 시도
            var section = document.getElementById('all-reviews') ||
                         document.querySelector('[id*="review"]') ||
                         document.querySelector('.reviews-container') ||
                         document.querySelector('div[class*="review"]');

            if (section) {
                section.scrollIntoView({behavior: 'smooth', block: 'center'});
                return true;
            }
            return false;
        """)

        if not section_exists:
            # 방법 2: 리뷰 컨테이너를 직접 찾아보기
            try:
                review_containers = driver.find_elements(By.CSS_SELECTOR,
                                                         'div.fragrance-review-box[itemprop="review"]')
                if review_containers:
                    safe_print(f"      ✅ {product_name}: 리뷰 컨테이너 직접 발견!")
                    section_exists = True
                    driver.execute_script(
                        "arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});",
                        review_containers[0]
                    )
                    time.sleep(2)
            except:
                pass

        if not section_exists:
            safe_print(f"      ℹ️  {product_name}: 리뷰 섹션 없음 -> 리뷰 0개")
            return []

        safe_print(f"      ✅ {product_name}: 리뷰 섹션 발견!")
        time.sleep(2)

        # 🔧 STEP 2: 리뷰 컨테이너 확인
        review_count = driver.execute_script("""
            var reviews = document.querySelectorAll('div.fragrance-review-box[itemprop="review"]');
            if (reviews.length === 0) {
                reviews = document.querySelectorAll('div[class*="review-box"]') ||
                         document.querySelectorAll('div[itemprop="review"]') ||
                         document.querySelectorAll('.review-container');
            }
            return reviews.length;
        """)

        safe_print(f"      ... {product_name}: {review_count}개 리뷰 컨테이너 감지됨")

        if review_count == 0:
            safe_print(f"      ℹ️  {product_name}: 리뷰 없음 -> 리뷰 0개")
            return []

        # 🔧 STEP 3: 무한 스크롤로 모든 리뷰 로드
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

        # 🔧 STEP 4: 모든 리뷰 추출
        review_elements = driver.find_elements(By.CSS_SELECTOR, 'div.fragrance-review-box[itemprop="review"]')

        # 대체 선택자 시도
        if not review_elements:
            safe_print(f"      ... {product_name}: 기본 선택자 실패, 대체 선택자 시도...")
            review_elements = driver.find_elements(By.CSS_SELECTOR, 'div[itemprop="review"]')

        if not review_elements:
            review_elements = driver.find_elements(By.CSS_SELECTOR, 'div[class*="review-box"]')

        safe_print(f"      ... {product_name}: {len(review_elements)}개 리뷰 추출 시작...")

        for idx, review in enumerate(review_elements, 1):
            try:
                # 리뷰어 이름
                reviewer_name_text = "Guest"
                try:
                    meta_name = review.find_element(By.CSS_SELECTOR, 'meta[itemprop="name"]')
                    reviewer_name_text = meta_name.get_attribute("content")
                except:
                    try:
                        reviewer_link = review.find_element(By.CSS_SELECTOR, 'a[href*="member"]')
                        reviewer_name_text = reviewer_link.text.strip()
                    except:
                        pass

                # 날짜
                review_date_text = "NA"
                try:
                    date_span = review.find_element(By.CSS_SELECTOR, 'span[itemprop="datePublished"]')
                    review_date_text = date_span.text.strip()
                except:
                    try:
                        date_meta = review.find_element(By.CSS_SELECTOR, 'meta[itemprop="datePublished"]')
                        review_date_text = date_meta.get_attribute("content")
                    except:
                        pass

                # 리뷰 내용
                content = ""
                try:
                    content_div = review.find_element(By.CSS_SELECTOR, 'div[itemprop="reviewBody"]')
                    paragraphs = content_div.find_elements(By.TAG_NAME, 'p')
                    content = " ".join([p.text.strip() for p in paragraphs if p.text.strip()])
                except:
                    try:
                        content_div = review.find_element(By.CSS_SELECTOR, 'div[itemprop="reviewBody"]')
                        content = content_div.text.strip()
                    except:
                        content = review.text.strip()

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

        try:
            current_url = driver.current_url
            safe_print(f"      ... 현재 URL: {current_url}")
        except:
            pass

        return []


# -----------------------
# 6. 워커 함수
# -----------------------

def process_single_product_reviews_only(args, driver_pool):
    """
    리뷰만 수집하는 워커 함수
    """
    url, product_name, index, total = args
    driver = None

    try:
        driver = driver_pool.get()

        safe_print(f"      ... {product_name}: 리뷰 수집 시작")

        # 리뷰 수집
        reviews_batch = scrape_reviews(driver, product_name, url)

        # CSV 저장
        if reviews_batch:
            write_batch_to_csv(REVIEW_CSV_FILE, REVIEW_FIELDNAMES, reviews_batch)

        # 딜레이
        delay = random.uniform(*RATE_LIMIT_DELAY_RANGE)
        safe_print(f"      ... 다음 작업까지 {delay:.1f}초 대기 ...")
        time.sleep(delay)

        # 드라이버 반환
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
            'product_name': product_name,
            'index': index,
            'total': total
        }


# -----------------------
# 7. 메인 함수
# -----------------------

def main_review_only():
    """
    기존 향수 목록 CSV에서 URL을 읽어와서 리뷰만 수집
    """
    start_time = time.time()

    print("=" * 60)
    print(f"🚀 Fragrantica 리뷰 전용 크롤러 시작")
    print(f"   (키워드: {SEARCH_KEYWORD})")
    print(f"   (드라이버 풀: {MAX_WORKERS}개)")
    print("=" * 60)

    # 1️⃣ 기존 향수 CSV 파일 확인
    if not os.path.exists(PERFUME_CSV_FILE):
        print(f"\n❌ 오류: '{PERFUME_CSV_FILE}' 파일이 존재하지 않습니다!")
        print(f"   먼저 향수 목록을 수집하거나, 파일명을 확인해주세요.")
        return

    # 2️⃣ 리뷰 CSV 파일 초기화
    try:
        if not os.path.exists(REVIEW_CSV_FILE):
            with open(REVIEW_CSV_FILE, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=REVIEW_FIELDNAMES)
                writer.writeheader()
            print(f"✅ 리뷰 CSV 파일 생성: {REVIEW_CSV_FILE}")
    except PermissionError as e:
        print("\n" + "!" * 60)
        print(f"❌ [치명적 오류] 파일 접근 권한이 없습니다: {e}")
        print(f"   '{REVIEW_CSV_FILE}' 파일이 Excel 등에서 열려있는지 확인하세요.")
        print("!" * 60 + "\n")
        sys.exit(1)

    # 3️⃣ 향수 목록 CSV에서 URL 읽기
    print(f"\n📂 '{PERFUME_CSV_FILE}'에서 URL 로딩 중...")
    product_data_list = []

    try:
        with open(PERFUME_CSV_FILE, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('url') and row.get('product_name'):
                    product_data_list.append({
                        'url': row['url'],
                        'product_name': row['product_name']
                    })
    except Exception as e:
        print(f"❌ CSV 파일 읽기 오류: {e}")
        return

    if not product_data_list:
        print(f"❌ '{PERFUME_CSV_FILE}'에서 유효한 URL을 찾지 못했습니다.")
        return

    print(f"✅ 총 {len(product_data_list)}개 제품 발견")

    # 4️⃣ 예상 시간 계산
    avg_delay = sum(RATE_LIMIT_DELAY_RANGE) / 2
    avg_time_per_product = 12 + avg_delay
    estimated_time_parallel = (len(product_data_list) * avg_time_per_product) / MAX_WORKERS
    print(f"\n📊 예상 소요 시간 ({MAX_WORKERS}개 병렬): 약 {estimated_time_parallel / 60:.1f}분")

    # 5️⃣ 드라이버 풀 초기화
    driver_pool = DriverPool(size=MAX_WORKERS)

    print("\n[리뷰 수집 시작]")
    print("-" * 60)

    scraping_start = time.time()
    total = len(product_data_list)

    # 6️⃣ 작업 준비
    tasks = [
        (item['url'], item['product_name'], i + 1, total)
        for i, item in enumerate(product_data_list)
    ]

    success_count = 0
    failed_count = 0
    total_reviews = 0

    # 7️⃣ 병렬 처리
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_single_product_reviews_only, task, driver_pool): task
            for task in tasks
        }

        for future in as_completed(futures):
            result = future.result()
            percentage = (result['index'] / result['total']) * 100

            if result['status'] == 'success':
                success_count += 1
                total_reviews += result['review_count']

                if result['review_count'] > 0:
                    safe_print(
                        f"[{result['index']}/{result['total']} ({percentage:.1f}%)] "
                        f"✅ {result['product_name']} - 리뷰 {result['review_count']}개"
                    )
                else:
                    safe_print(
                        f"[{result['index']}/{result['total']} ({percentage:.1f}%)] "
                        f"ℹ️  {result['product_name']} - 리뷰 없음"
                    )
            else:
                failed_count += 1
                safe_print(
                    f"[{result['index']}/{result['total']} ({percentage:.1f}%)] "
                    f"❌ {result['product_name']} - 처리 실패: {result['error']}"
                )

    # 8️⃣ 드라이버 풀 종료
    print("\n🔧 드라이버 풀 종료 중...")
    driver_pool.close_all()

    scraping_time = time.time() - scraping_start
    total_time = time.time() - start_time

    # 9️⃣ 최종 결과 출력
    print("-" * 60)
    print("\n" + "=" * 60)
    print("✅ 리뷰 수집 완료!")
    print("=" * 60)
    print(f"\n📊 통계:")
    print(f"   - 성공: {success_count}개")
    print(f"   - 실패: {failed_count}개")
    print(f"   - 총 리뷰 수: {total_reviews}개")
    print(f"\n⏱️  소요 시간:")
    print(f"   - 리뷰 수집: {scraping_time / 60:.1f}분")
    print(f"   - 전체: {total_time / 60:.1f}분")
    print(f"\n📁 저장된 파일:")
    print(f"   - {REVIEW_CSV_FILE}")
    print("=" * 60)


# -----------------------
# 8. 실행
# -----------------------

if __name__ == "__main__":
    main_review_only()