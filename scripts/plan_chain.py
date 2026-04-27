import argparse

PROVINCE_IDS = [
    '11', '12', '13', '14', '15',
    '21', '22', '23',
    '31', '32', '33', '34', '35', '36', '37',
    '41', '42', '43',
    '44', '45', '46',
    '50', '51', '52', '53', '54',
    '61', '62', '63', '64', '65',
    '71', '81', '82',
]


def parse_years(raw: str):
    if not raw:
        return []
    return [item.strip() for item in str(raw).split(',') if item.strip()]


def normalize_index(value: str):
    raw = (value or '').strip()
    if raw == '':
        return None
    idx = int(raw)
    if idx < 0 or idx >= len(PROVINCE_IDS):
        raise ValueError(f'无效省份索引: {value}')
    return idx


def next_pair(years, current_year, current_province_index):
    if current_province_index + 1 < len(PROVINCE_IDS):
        return current_year, current_province_index + 1

    try:
        year_index = years.index(current_year)
    except ValueError as exc:
        raise ValueError(f'当前年份不在计划范围内: {current_year}') from exc

    if year_index + 1 >= len(years):
        return '', None

    return years[year_index + 1], 0


def write_output(path: str, key: str, value: str):
    if not path:
        return
    with open(path, 'a', encoding='utf-8') as f:
        f.write(f'{key}={value}\n')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--years', required=True)
    parser.add_argument('--current-year', default='')
    parser.add_argument('--current-province-index', default='')
    parser.add_argument('--github-output', default='')
    args = parser.parse_args()

    years = parse_years(args.years)
    if not years:
        raise SystemExit('没有可执行年份')

    current_year = (args.current_year or '').strip()
    current_province_index = normalize_index(args.current_province_index)

    if not current_year:
        current_year = years[0]
        current_province_index = 0
    elif current_year not in years:
        raise SystemExit(f'当前年份不在计划范围内: {current_year}')
    elif current_province_index is None:
        current_province_index = 0

    current_province_id = PROVINCE_IDS[current_province_index]
    next_year, next_province_index = next_pair(years, current_year, current_province_index)
    next_province_id = PROVINCE_IDS[next_province_index] if next_province_index is not None else ''

    outputs = {
        'current_year': current_year,
        'current_province_index': str(current_province_index),
        'current_province_id': current_province_id,
        'next_year': next_year,
        'next_province_index': '' if next_province_index is None else str(next_province_index),
        'next_province_id': next_province_id,
    }

    for key, value in outputs.items():
        write_output(args.github_output, key, value)

    for key, value in outputs.items():
        print(f'{key}={value}')


if __name__ == '__main__':
    main()
