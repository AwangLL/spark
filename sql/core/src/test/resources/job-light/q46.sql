SELECT COUNT(*)
FROM title t,
     movie_info mi,
     movie_info_idx mi_idx,
     cast_info ci
WHERE t.id=mi.movie_id
  AND t.id=mi_idx.movie_id
  AND t.id=ci.movie_id
  AND mi.info_type_id=3
  AND mi_idx.info_type_id=101
  AND t.production_year>2008
  AND t.production_year<2014;