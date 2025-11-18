from math import log


def safe(val, default=0.0):
    return default if val is None else val


def clamp01(x, lo=0.0, hi=1.0):
    if hi == lo: return 0.0
    return max(lo, min(hi, x)) if lo < hi else 0.0


def computeScores(parsed, est_sales_per_month=None, rank_cap=1_000_000):
    """
    parsed = {
        'buy_box_price': float,
        'cogs': float,
        'lowest_price': float,
        'number_of_offers': int,
        'competitive_price': float,
        'amazon_on_listing': bool,
        'fba_sellers': int,
        'fbm_sellers': int,
        'fba_fees': float,
        'fbm_fees': float,
        'sales_ranks': [{'ProductCategoryId': 'XXXX', 'Rank': int}, ...]
    }
    """

    buy_box = safe(parsed.get('buy_box_price'))
    cogs = safe(parsed.get('cogs'))
    lowest = safe(parsed.get('lowest_price')) or buy_box
    offers = int(safe(parsed.get('number_of_offers'), 0))
    comp_price = safe(parsed.get('competitive_price'))
    amazon_retail = bool(parsed.get('amazon_on_listing', False))
    fba_sellers = int(safe(parsed.get('fba_sellers'), 0))
    fbm_sellers = int(safe(parsed.get('fbm_sellers'), 0))
    fba_fees = safe(parsed.get('fba_fees'))
    fbm_fees = safe(parsed.get('fbm_fees'))

    ranks = [r.get('Rank') for r in parsed.get('sales_ranks', []) if
             isinstance(r.get('Rank'), int) and r.get('Rank') > 0]
    best_rank = min(ranks) if ranks else None
    if best_rank:
        rank_demand_index = clamp01(1 - (log(best_rank) / log(rank_cap)))
    else:
        rank_demand_index = 0.0

    fba_cost = cogs + fba_fees
    fbm_cost = cogs + fbm_fees
    fba_profit = buy_box - fba_cost
    fbm_profit = buy_box - fbm_cost
    fba_roi = (fba_profit / max(1e-9, cogs))  # avoid divide-by-zero
    fba_margin_pct = fba_profit / max(1e-9, buy_box)

    fbm_roi = (fbm_profit / max(1e-9, cogs))  # avoid divide-by-zero
    fbm_margin_pct = fbm_profit / max(1e-9, buy_box)

    bb_proximity = (buy_box - comp_price) / buy_box if buy_box and comp_price else 0.0
    price_spread = buy_box - lowest
    fba_share = fba_sellers / max(1, (fba_sellers + fbm_sellers))

    revenue_velocity = (est_sales_per_month * buy_box) if est_sales_per_month else None

    score_fba = (
            40 * rank_demand_index +
            20 * clamp01(fba_roi, 0, 1.0) +
            15 * clamp01((bb_proximity + 0.1) / 0.2) +
            10 * (1 / (1 + offers)) * 10 -
            (50 if amazon_retail else 0)
    )
    score_fba = clamp01(score_fba / 100.0) * 100

    score_fbm = (
            40 * rank_demand_index +
            20 * clamp01(fbm_roi, 0, 1.0) +
            15 * clamp01((bb_proximity + 0.1) / 0.2) +
            10 * (1 / (1 + offers)) * 10 -
            (50 if amazon_retail else 0)
    )
    score_fbm = clamp01(score_fbm / 100.0) * 100

    return score_fba, score_fbm
