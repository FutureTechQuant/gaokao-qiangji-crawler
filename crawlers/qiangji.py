import json
import os
import time
from pathlib import Path

from .base import BaseCrawler


class QiangjiCrawler(BaseCrawler):
    def __init__(self):
        super().__init__()
        self._first_logged = False
        self.progress_dir = Path(os.getenv('QIANGJI_PROGRESS_DIR', 'data/qiangji_progress'))
        self.data_dir = Path(os.getenv('QIANGJI_DATA_DIR', 'data/qiangji'))
        self.run_deadline_seconds = int(os.getenv('QIANGJI_RUN_DEADLINE_SECONDS', '17400'))
        self.flush_schools = max(1, int(os.getenv('QIANGJI_FLUSH_SCHOOLS', '25')))

    def now_str(self):
        return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

    def write_json_atomic(self, path, payload):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(path.suffix + '.tmp')
        with open(tmp_path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, path)

    def format_duration(self, seconds):
        seconds = max(0, float(seconds))
        hours, remainder = divmod(int(seconds), 3600)
        minutes, secs = divmod(remainder, 60)
        if hours > 0:
            return f'{hours}小时{minutes}分{secs}秒'
        if minutes > 0:
            return f'{minutes}分{secs}秒'
        return f'{seconds:.2f}秒'

    def load_default_schools(self):
        schools_file = Path(os.getenv('SCHOOL_DATA_FILE', 'data/schools.json'))
        if not schools_file.exists():
            print(f'⚠️  未找到 schools.json: {schools_file}')
            return []

        with open(schools_file, 'r', encoding='utf-8') as f:
            payload = json.load(f)

        if isinstance(payload, list):
            schools = payload
        elif isinstance(payload, dict):
            schools = payload.get('data', [])
            if not schools and payload.get('school_id'):
                schools = [payload]
        else:
            schools = []

        items = []
        for item in schools:
            if not isinstance(item, dict) or not item.get('school_id'):
                continue
            items.append({
                'school_id': str(item.get('school_id')),
                'school_name': item.get('name') or item.get('school_name') or item.get('school_name_cn') or '',
            })

        def sort_key(x):
            sid = x['school_id']
            return (0, int(sid)) if sid.isdigit() else (1, sid)

        return sorted({item['school_id']: item for item in items}.values(), key=sort_key)

    def get_progress_file(self):
        custom = os.getenv('QIANGJI_PROGRESS_FILE', '').strip()
        if custom:
            return Path(custom)
        return self.progress_dir / 'progress.json'

    def load_progress(self, target_school_ids):
        path = self.get_progress_file()
        base = {
            'target_school_ids': [str(x) for x in target_school_ids],
            'current_school_index': 0,
            'updated_at': None,
            'last_error': None,
            'status': 'new',
        }
        if not path.exists():
            return base
        try:
            with open(path, 'r', encoding='utf-8') as f:
                progress = json.load(f)
        except Exception:
            return base

        saved_targets = [str(x) for x in progress.get('target_school_ids', [])]
        current_targets = [str(x) for x in target_school_ids]
        if saved_targets != current_targets:
            return base
        return progress

    def save_progress(self, target_school_ids, current_school_index, last_error=None, status='running'):
        payload = {
            'target_school_ids': [str(x) for x in target_school_ids],
            'current_school_index': int(current_school_index),
            'updated_at': self.now_str(),
            'last_error': last_error,
            'status': status,
        }
        self.write_json_atomic(self.get_progress_file(), payload)

    def clear_progress(self):
        path = self.get_progress_file()
        if path.exists():
            path.unlink()

    def get_school_file_path(self, school_id):
        return self.data_dir / f'{school_id}.json'

    def get_qiangji_payload(self, school_id):
        url = f'https://static-data.gaokao.cn/www/2.0/qiangji/school/{school_id}/info.json?a=www.gaokao.cn'
        try:
            response = self.session.get(url, timeout=10)
            if response.status_code == 200:
                result = response.json()
                if result.get('code') == '0000' and 'data' in result:
                    return result
            elif response.status_code == 404:
                return 'no_data'
        except Exception as e:
            print(f'      ⚠️  获取强基数据失败 (ID:{school_id}): {str(e)}')
        return None

    def count_schedule_events(self, schedule):
        total = 0
        if not isinstance(schedule, dict):
            return total
        for months in schedule.values():
            if not isinstance(months, dict):
                continue
            for events in months.values():
                if isinstance(events, list):
                    total += len(events)
        return total

    def normalize_school_payload(self, school, result):
        school_id = str(school.get('school_id'))
        fallback_school_name = school.get('school_name') or ''
        data = result.get('data') or {}
        school_info = data.get('school') or {}
        year_list = data.get('year') or []
        formula = data.get('formula') or {}
        schedule = data.get('schedule') or {}

        return {
            'update_time': self.now_str(),
            'school_id': school_id,
            'school_name': school_info.get('school_name') or fallback_school_name,
            'source_url': f'https://static-data.gaokao.cn/www/2.0/qiangji/school/{school_id}/info.json?a=www.gaokao.cn',
            'time': result.get('time'),
            'md5': result.get('md5'),
            'year_count': len(year_list) if isinstance(year_list, list) else 0,
            'formula_years': sorted(list(formula.keys()), reverse=True) if isinstance(formula, dict) else [],
            'schedule_years': sorted(list(schedule.keys()), reverse=True) if isinstance(schedule, dict) else [],
            'schedule_event_count': self.count_schedule_events(schedule),
            'data': data,
        }

    def save_school_payload(self, payload):
        self.write_json_atomic(self.get_school_file_path(payload['school_id']), payload)

    def should_stop(self, started_at):
        return (time.time() - started_at) >= self.run_deadline_seconds

    def crawl(self, schools=None):
        schools = schools or self.load_default_schools()
        target_school_ids = [str(item['school_id']) for item in schools]

        if not schools:
            print('⚠️  没有可用学校ID')
            return {
                'status': 'skipped',
                'saved_documents': 0,
                'completed_schools': 0,
            }

        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.progress_dir.mkdir(parents=True, exist_ok=True)

        progress = self.load_progress(target_school_ids)
        start_index = int(progress.get('current_school_index', 0) or 0)
        started_at = time.time()
        saved_count = 0

        print("\n" + "=" * 60)
        print('启动强基计划数据爬虫')
        print(f'学校数: {len(schools)}')
        print(f'软截止: {self.format_duration(self.run_deadline_seconds)}')
        print(f'学校起始索引: {start_index + 1}/{len(schools)}')
        print("=" * 60 + "\n")

        for school_index in range(start_index, len(schools)):
            if self.should_stop(started_at):
                self.save_progress(
                    target_school_ids=target_school_ids,
                    current_school_index=school_index,
                    last_error='run deadline reached',
                    status='partial',
                )
                print('⏸️ 接近 5 小时上限，已保存 progress，准备下一轮续跑')
                return {
                    'status': 'partial',
                    'saved_documents': saved_count,
                    'completed_schools': school_index,
                }

            school = schools[school_index]
            school_id = str(school['school_id'])
            school_name = school.get('school_name') or '未知学校'
            print(f'[{school_index + 1}/{len(schools)}] 学校ID: {school_id}', end='', flush=True)

            result = self.get_qiangji_payload(school_id)
            if not result or result == 'no_data' or not isinstance(result, dict):
                print(f' ✗ {school_name} - 无强基数据')
                self.polite_sleep(0.2, 0.6)
                continue

            data = result.get('data') or {}
            year_list = data.get('year') or []
            school_info = data.get('school') or {}
            formula = data.get('formula') or {}
            schedule = data.get('schedule') or {}

            if not self._first_logged:
                sample_formula = {}
                if isinstance(formula, dict) and formula:
                    first_year = sorted(formula.keys(), reverse=True)[0]
                    if isinstance(formula.get(first_year), dict):
                        sample_formula = formula.get(first_year, {})
                print(f'\n\n   📡 [强基接口] school_id={school_id}')
                print(f'      URL: https://static-data.gaokao.cn/www/2.0/qiangji/school/{school_id}/info.json?a=www.gaokao.cn')
                print('      ' + '─' * 50)
                print(f"      顶层键: {list(result.keys())}")
                print(f"      data键: {list(data.keys()) if isinstance(data, dict) else []}")
                print(f"      年份数: {len(year_list) if isinstance(year_list, list) else 0}")
                print(f"      formula年份: {sorted(list(formula.keys()), reverse=True) if isinstance(formula, dict) else []}")
                print(f"      schedule年份: {sorted(list(schedule.keys()), reverse=True) if isinstance(schedule, dict) else []}")
                print(f"      schedule事件数: {self.count_schedule_events(schedule)}")
                print(f"      学校名称: {school_info.get('school_name', school_name)}")
                print(f"      formula样例字段: {list(sample_formula.keys()) if isinstance(sample_formula, dict) else []}")
                print('      ' + '─' * 50 + '\n')
                self._first_logged = True

            payload = self.normalize_school_payload(school, result)
            self.save_school_payload(payload)
            saved_count += 1

            print(f" ✓ {payload.get('school_name', school_name)} - 年份 {payload.get('year_count', 0)} 个")

            if (school_index + 1) % self.flush_schools == 0:
                self.save_progress(
                    target_school_ids=target_school_ids,
                    current_school_index=school_index + 1,
                    last_error=None,
                    status='running',
                )
                print(f'\n   ↻ 已阶段性保存：学校进度 {school_index + 1}/{len(schools)}，已写入 {saved_count} 个文件\n')

            self.polite_sleep(0.2, 0.6)

        self.clear_progress()
        total_files = len(list(self.data_dir.glob('*.json')))

        print("\n" + "=" * 60)
        print('✅ 强基计划数据爬取完成！')
        print(f'   本轮写入: {saved_count} 个文件')
        print(f'   累计文件: {total_files} 个')
        print("=" * 60 + "\n")
        return {
            'status': 'done',
            'saved_documents': total_files,
            'completed_schools': len(schools),
        }


if __name__ == '__main__':
    crawler = QiangjiCrawler()
    crawler.crawl()
