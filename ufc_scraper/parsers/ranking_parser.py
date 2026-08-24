import logging
from ..utils.ranking_mappings import WEIGHT_CLASS_MAPPING, NAME_EXCEPTIONS
from ..items import RankingItem

logger = logging.getLogger(__name__)


def parse_rankings(response, fighter_cache):
    groupings = response.css('.block-views-blockathlete-rankings-block-1 .view-grouping')

    for group in groupings:
        raw_title = group.css('.view-grouping-header::text').get()
        if not raw_title:
            continue

        title = raw_title.strip()
        db_weight_class_id = WEIGHT_CLASS_MAPPING.get(title)

        if not db_weight_class_id:
            logger.warning(f"Weight class not matched: {title}")
            continue

        champion_name = group.css('.info h5 a::text').get()
        if champion_name:
            item = _process_fighter(champion_name, db_weight_class_id, 0, 0, fighter_cache)
            if item:
                yield item

        rows = group.css('tbody tr')
        current_rank = 1

        for row in rows:
            fighter_name = row.css('.views-field-title a::text').get()

            rank_change = 0
            rank_change_td = row.css('.views-field-weight-class-rank-change')
            if rank_change_td:
                change_texts = rank_change_td.xpath('./text()').getall()
                change_text = "".join(change_texts).strip().replace('"', '')
                change_span_class = rank_change_td.css('span::attr(class)').get()

                if change_span_class and 'not-ranked' in change_span_class:
                    rank_change = None
                elif change_text and change_span_class:
                    try:
                        change_val = int(change_text)
                        if 'increase' in change_span_class:
                            rank_change = change_val
                        elif 'decrease' in change_span_class:
                            rank_change = -change_val
                    except ValueError:
                        pass

            if fighter_name:
                item = _process_fighter(fighter_name, db_weight_class_id, current_rank, rank_change, fighter_cache)
                if item:
                    yield item
                current_rank += 1


def _process_fighter(fighter_name, weight_class_id, rank, rank_change, fighter_cache):
    if rank == 0 and weight_class_id in ["mens_p4p", "womens_p4p"]:
        return None

    fighter_name = fighter_name.strip()

    search_name = NAME_EXCEPTIONS.get(fighter_name, fighter_name)
    found_id = fighter_cache.get(search_name)

    if found_id:
        return RankingItem(
            item_type="ranking",
            weight_class_id=weight_class_id,
            fighter_id=found_id,
            rank_number=rank,
            rank_change=rank_change,
        )
    else:
        logger.warning(f"Fighter not found in DB: {fighter_name}")
        return None
