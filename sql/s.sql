CREATE OR REPLACE PROCEDURE refresh_fs_tables(IN force BOOLEAN DEFAULT FALSE)
AS $$
DECLARE
  hash_value TEXT;
  property_value TEXT;
BEGIN
  -- Step 0: Create the properties table if it doesn't exist
  EXECUTE 'CREATE TABLE IF NOT EXISTS properties_table (property_key TEXT PRIMARY KEY, property_value TEXT)';

  -- Step 1: Compute the accumulated hash value for the table (when force is false)
   IF NOT force THEN
     SELECT COALESCE(MD5(string_agg(wkt, '')), '') INTO hash_value
     FROM input_wkt;

     -- Step 2: Retrieve the current hash value from the properties table
     SELECT properties_table.property_value INTO property_value
     FROM properties_table
     WHERE property_key = 'fs_wkt_input';
  END IF;


  -- Step 3-4: Compare with the stored hash value and update if different
  IF NOT force AND (property_value IS NULL OR hash_value <> property_value) THEN
    -- Step 4: Update the hash value in the properties table
    EXECUTE 'INSERT INTO properties_table (property_key, property_value) VALUES ($1, $2)
             ON CONFLICT (property_key) DO UPDATE SET property_value = EXCLUDED.property_value'
    USING 'fs_wkt_input', hash_value;
  END IF;

  -- Step 5: Call the stored procedure or perform other actions as needed
  IF force OR (property_value is NULL OR hash_value <> property_value) THEN
    call delete_fs_views();
    call create_fs_views();
    -- REFRESH MATERIALIZED VIEW tnroadi;
  END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE create_fs_views()
AS $$
DECLARE
  hash_value TEXT;
  property_value TEXT;
BEGIN
  CREATE OR REPLACE VIEW fsv_congs as
  select 'KINGDOMHALL' kh,ST_Union(st_buffer(geom,0)) geom from fst_cong
  where active;
  
CREATE OR REPLACE VIEW fsv_road as
WITH cong AS (
    WITH a AS (
      SELECT
        fst_cong.id cname,
        fst_cong.geom,active,
        buffer_distance_feet / (feet_per_degree * cos(radians(ST_Y(ST_StartPoint(fst_cong.geom))))) AS buffer_distance_degrees
      FROM fst_cong
      CROSS JOIN (
        SELECT 
          55.0 AS buffer_distance_feet,  -- feet
          364259.9462 AS feet_per_degree  -- FOUND ON internet... yay!
      ) AS params
    )
    SELECT a.cname, ST_Buffer(a.geom, buffer_distance_degrees) geom,active
    FROM a
)
select COALESCE(raa.primename,r.primename) oprimename,r.*
  from (
       select r.* from fst_road r, cong c
       where ST_contains(c.geom,r.geom) and c.active
       union all
       select r.* from fst_road r,
    cong c
       where ST_intersects(c.geom,r.geom) and active and exists(
             select 1 from fst_addr a, fsv_addr2roadside rs where rs.road_id = r.id and st_contains(c.geom,a.geom) and rs.addr_id = a.id
       )
  ) r
  left join fsv_roadaddralias raa on raa.road_id = r.id;

DELETE FROM fst_dnc;
INSERT INTO fst_dnc (tmpstmp, lang, zip, primename, add_number, rsn, remark)
SELECT COALESCE(tstmp::TIMESTAMP,NOW()), lang,zip,primename,add_number,rsn,remark FROM fsv_dnc;

WITH a AS (
    SELECT post_code zip,primename,add_number,min(id) id FROM fst_addr a
    WHERE post_code IS NOT NULL AND primename IS NOT NULL AND add_number IS NOT NULL
    GROUP BY 1,2,3
    HAVING count(*)=1
)
UPDATE fst_dnc d
  SET addr_id =  a.id
  FROM  a
    WHERE d.zip = a.zip AND d.primename  = a.primename 
        AND d.add_number = a.add_number;

CREATE OR REPLACE VIEW fsv_street AS 
WITH merged_lines AS (
  SELECT primename, ST_Union(geom) AS geom
  FROM fsv_road
  GROUP BY primename
), buffer_params AS (
  SELECT
    merged_lines.primename,
    merged_lines.geom,
    buffer_distance_feet / (feet_per_degree * cos(radians(ST_Y(ST_StartPoint(merged_lines.geom))))) AS buffer_distance_degrees
  FROM merged_lines
  CROSS JOIN (
    SELECT 
      10.0 AS buffer_distance_feet,  -- feet
      364259.9462 AS feet_per_degree  -- FOUND ON internet... yay!
  ) AS params
) 
, unwrapped_polygons AS MATERIALIZED (
  SELECT b.primename,c.cname,c.geom cgeom,
         (ST_Polygonize(ST_Buffer(b.geom, b.buffer_distance_degrees,'endcap=square join=mitre mitre_limit=1.0'))) AS geom,
         min(buffer_distance_degrees) buffer_distance_degrees
  FROM buffer_params b,(
    WITH a AS (
      SELECT
        fst_cong.id cname,
        fst_cong.geom,
        buffer_distance_feet / (feet_per_degree * cos(radians(ST_Y(ST_StartPoint(fst_cong.geom))))) AS buffer_distance_degrees
      FROM fst_cong
      CROSS JOIN (
        SELECT 
          15.0 AS buffer_distance_feet,  -- feet
          364259.9462 AS feet_per_degree  -- FOUND ON internet... yay!
      ) AS params
    )
    SELECT a.cname, ST_Buffer(a.geom, buffer_distance_degrees) geom
    FROM a
  ) c 
  GROUP BY b.primename, c.cname, c.geom
)
  SELECT *
  FROM (
    SELECT
      min(r.id) id,
      CASE
        WHEN st_contains(p.cgeom, g.dump) OR (ST_Intersects(p.cgeom, g.dump) AND NOT ST_Contains(p.cgeom, g.dump) AND 
          ST_Area(ST_Difference(g.dump, p.cgeom)) / NULLIF(ST_Area(g.dump), 0) < 0.1)
        THEN g.dump
        ELSE ST_Intersection(g.dump, p.cgeom)
      END AS geom,
      p.cname, p.primename, p.cgeom AS cgeom
    FROM unwrapped_polygons p
    JOIN fsv_road r ON (p.primename = r.primename OR (p.primename IS NULL AND r.primename IS NULL))  
    JOIN LATERAL (
      SELECT (ST_Dump(p.geom)).geom AS dump
    ) g ON true
    WHERE ST_Contains(p.geom, r.geom) AND ST_Contains(p.cgeom, r.geom)
    GROUP BY g.dump, p.cname, p.primename, p.cgeom
  ) p
  WHERE st_contains(p.cgeom, p.geom) OR (ST_Intersects(p.cgeom, p.geom) AND NOT ST_Contains(p.cgeom, p.geom) AND 
    ST_Area(ST_Difference(p.geom, p.cgeom)) / NULLIF(ST_Area(p.geom), 0) < 0.1);

CREATE OR REPLACE VIEW fsv_iw AS
WITH cte_cong AS MATERIALIZED (
WITH RECURSIVE intersection_areas AS (
    SELECT
        a.id AS id_a,
        b.id AS id_b,
        1 AS iteration,
        ST_Intersection(a.geom, b.geom) AS intersection_geom
    FROM
        fst_cong AS a
    JOIN
        fst_cong AS b ON ST_Intersects(a.geom, b.geom) AND a.id < b.id
    WHERE
        ST_IsValid(a.geom) AND ST_IsValid(b.geom)
    UNION ALL
    SELECT
        ia.id_a,
        ia.id_b,
        ia.iteration + 1,
        ST_Intersection(ia.intersection_geom, c.geom) AS intersection_geom
    FROM
        intersection_areas AS ia
    JOIN
        fst_cong AS c ON ST_Intersects(ia.intersection_geom, c.geom) AND c.id <> ia.id_a AND c.id <> ia.id_b
    WHERE
        ST_IsValid(c.geom)
),
polygon_areas AS (
    SELECT
        id,
        ST_Area(geom) AS area
    FROM
        fst_cong
    WHERE
        ST_IsValid(geom)
)
SELECT
    p.id,
    CASE
        WHEN polygon_areas.area - COALESCE(SUM(ST_Area(intersection_areas.intersection_geom)), 0) > 0
            THEN ST_Union(p.geom, ST_MakeValid(intersection_areas.intersection_geom))
        ELSE p.geom
    END AS geom, p.cardprefix
FROM
    fst_cong AS p
LEFT JOIN
    intersection_areas ON p.id = intersection_areas.id_a OR p.id = intersection_areas.id_b
LEFT JOIN
    polygon_areas ON p.id = polygon_areas.id
GROUP BY
    p.id, p.geom, polygon_areas.area,intersection_areas.intersection_geom
), ca AS (
SELECT
    ca.id,
    ca.cardprefix || to_char(ROW_NUMBER() OVER (PARTITION BY ca.cardprefix ORDER BY ca.id), 'fm000') tnum,
    ca.rotate,
    ca.scale,
    ca.cardtype,
    ca.geom,
    st_convexhull(ca.geom) chgeom,
    ca.locale,
    ca.cname
FROM
(
WITH borderline_polygons AS (
SELECT a.id,a.geom,a.cardtype,a.locale,a.rotate,a.scale,c.cardprefix,c.id cname FROM fst_cardatlas a
JOIN cte_cong c ON st_isvalid(a.geom) AND NOT st_contains(c.geom,a.geom) AND st_intersects(c.geom,a.geom)
AND c.id =  (
SELECT cc.id 
FROM cte_cong cc
ORDER BY st_area(st_intersection(st_buffer(cc.geom,0),a.geom)) DESC,cc.id
LIMIT 1
)
), aa AS (
SELECT a.* FROM borderline_polygons a WHERE  EXISTS (
    SELECT 1 FROM borderline_polygons b WHERE b.id <> a.id AND st_intersects(a.geom,b.geom)  AND a.cname <> b.cname
)),
bb AS (
    SELECT aa.id,ST_Difference(aa.geom,ST_Difference(ST_Union(bb.geom),ST_Buffer(c.geom,0))) geom
    FROM aa, aa bb,cte_cong c
    WHERE aa.cname <> bb.cname AND c.id = aa.cname
    GROUP BY 1,aa.geom,c.geom
)
SELECT a.id,a.geom,a.cardtype,a.locale,a.rotate,a.scale,c.cardprefix, c.id cname FROM fst_cardatlas a
JOIN cte_cong c ON st_contains(c.geom,a.geom)
UNION ALL
SELECT a.* FROM borderline_polygons a WHERE NOT EXISTS (
    SELECT 1 FROM borderline_polygons b WHERE b.id <> a.id AND st_intersects(a.geom,b.geom) AND a.cname <> b.cname
)
UNION ALL
SELECT aa.id,bb.geom,aa.cardtype,aa.locale,aa.rotate,aa.SCALE,aa.cardprefix,aa.cname
FROM aa,bb,fsv_street s WHERE aa.id= bb.id AND aa.cname = s.cname AND st_intersects(aa.geom,s.geom)
) ca
WHERE ca.geom IS NOT null
)
SELECT DISTINCT ON (ca.tnum) ca.*,COALESCE (ca.locale,a.post_comm_list) post_comm_list
FROM ca
LEFT JOIN (
    SELECT ca.id,string_agg(DISTINCT a.post_comm, ' & ' ORDER BY a.post_comm) AS post_comm_list
    FROM ca
    JOIN fst_addr a ON st_contains(ca.geom,a.geom)
    GROUP BY 1
) a ON a.id = ca.id;

CREATE OR REPLACE VIEW fsv_cardatlas_cnt AS
WITH aa AS MATERIALIZED (
    SELECT max(cc.id) cid ,rr.id road_id FROM fst_cardatlas cc, fst_road rr WHERE st_intersects(cc.geom,rr.geom) GROUP BY rr.id
)
SELECT c.id, COUNT(distinct rs.addr_id) AS count
FROM fst_cardatlas c
JOIN aa ON aa.cid = c.id
JOIN fsv_addr2roadside rs ON rs.road_id = aa.road_id
JOIN fst_addr a ON rs.addr_id = a.id AND a.place_type = 'Residence'
GROUP BY c.id;

CREATE MATERIALIZED VIEW fsv_terrroad AS
with a as (
SELECT DISTINCT ON (r.id) r.*, iw.tnum,
iw.rotate,
iw.SCALE, s.cname,s.id street_id
FROM fst_road r
join fsv_street s on st_contains(s.geom,r.geom)  AND s.primename = r.primename
JOIN fsv_iw iw ON (st_intersects(iw.geom, r.geom))
WHERE iw.cardtype = 'S'
ORDER BY r.id,iw.id desc
), b AS (
select * from a
union all
select DISTINCT r.*,
NULL,
NULL::double precision,
NULL::double precision, s.cname,s.id street_id
from fst_road r,fsv_street s
where r.id not in (select id from a) and st_intersects(s.geom,r.geom) AND s.primename = r.primename
)
SELECT DISTINCT ON (b.id,b.cname) b.*,c.color FROM b
LEFT JOIN colors c ON c.tnum = b.tnum;



create or replace view ntaddr as
select tr.tnum,ST_Azimuth(a.geom, ST_ClosestPoint(r.geom, a.geom)) rotate,
a.*,tr.street_id,tr.cname from fsv_terrroad tr, fsv_addr2roadside rs,fst_addr a ,fst_cong c, fst_road r
where c.active and st_Intersects(c.geom,a.geom) and rs.road_id = tr.id and a.id = rs.addr_id and a.place_type = 'Residence' and r.id = rs.road_id
AND tr.cname = c.id;




create or replace view  fsv_ua as 
SELECT a.* FROM fst_addr a, fst_cong c WHERE (st_intersects(c.geom, a.geom) AND ((a.place_type)::text = 'Residence'::text) AND c.active AND (NOT (EXISTS ( SELECT 1 FROM fsv_terrroad tr, fsv_addr2roadside rs WHERE ((tr.id = rs.road_id) AND (a.id = rs.addr_id))))));

create or replace view fsv_ur as 
select distinct on (r.id)  a.id addr_id,r.* from fst_road r,fsv_addr2roadside  rs, fsv_ua a where r.id = rs.road_id and rs.addr_id = a.id  order by r.id;


create materialized view tnroadi as 
WITH a AS MATERIALIZED (
SELECT a.add_number % 2 = 0 AS is_even, tr.tnum,tr.street_id, a.*
    FROM fst_addr a
    join fsv_addr2roadside rs ON rs.addr_id = a.id AND a.place_type = 'Residence'
    JOIN fsv_terrroad tr ON tr.id = rs.road_id AND tr.primename = a.primename
),
b AS MATERIALIZED (
    SELECT a.tnum, a.street_id,is_even,  primename, MIN(add_number) AS lo, MAX(add_number) AS hi
    FROM a
    GROUP BY a.tnum, a.street_id,is_even, primename
)
,g AS MATERIALIZED (
    SELECT tnum, street_id, primename , COUNT(*) AS cnt
    FROM a
    GROUP BY tnum, street_id,primename
)
,e AS MATERIALIZED (
    SELECT DISTINCT b.tnum,g.street_id, COALESCE(b.primename,'{unnamed}') primename, o.lo AS odd_low, o.hi AS odd_hi, e.lo AS even_low, e.hi AS even_hi, g.cnt
    FROM b
    LEFT JOIN (SELECT * FROM b WHERE is_even) e ON (b.tnum = e.tnum AND b.primename = e.primename AND b.street_id = e.street_id)
    LEFT JOIN (SELECT * FROM b WHERE NOT is_even) o ON (b.tnum = o.tnum AND b.primename = o.primename AND b.street_id = o.street_id)
    LEFT JOIN g ON (b.tnum = g.tnum AND b.primename = g.primename AND b.street_id = g.street_id)
) 
,excluded_primenames AS MATERIALIZED (
  SELECT DISTINCT r.primename
  FROM fst_cong c
  JOIN fst_road r ON ST_Crosses(r.geom, c.geom)
  WHERE c.active AND r.primename IS NOT NULL
),
one_road_tnum AS MATERIALIZED (
  SELECT r.primename,
         MAX(CASE WHEN a.add_number % 2 = 0 THEN r.tnum END) AS even_tnum,
         MAX(CASE WHEN a.add_number % 2 <> 0 THEN r.tnum END) AS odd_tnum
  FROM fsv_addr2roadside rs
  JOIN fsv_terrroad r ON rs.road_id = r.id
  JOIN fst_addr a ON rs.addr_id = a.id
  GROUP BY r.primename
  HAVING COUNT(DISTINCT r.street_id) = 1  AND 
  COUNT(DISTINCT CASE WHEN a.add_number % 2 = 0 THEN r.tnum END) <= 1
     AND COUNT(DISTINCT CASE WHEN a.add_number % 2 <> 0 THEN r.tnum END) <= 1
)
, f as MATERIALIZED (
SELECT *,
    CASE
        WHEN cnt > 4 AND odd_low <> odd_hi AND even_low <> even_hi THEN
            CASE
                WHEN odd_low < even_low THEN CONCAT(odd_low, '/', even_low, ' to ', odd_hi, '/', even_hi)
                ELSE CONCAT(even_low, '/', odd_low, ' to ', even_hi, '/', odd_hi)
            END
        WHEN cnt > 4  AND  odd_low <> odd_hi and even_low is null  THEN CONCAT('Odd ',odd_low, ' to ', odd_hi)
        WHEN cnt >4  AND  even_low <> even_hi and odd_low is null  THEN CONCAT('Even ',even_low, ' to ', even_hi)
    WHEN cnt <=4 THEN
        CONCAT ('Only ',( 
SELECT string_agg(value::text, ', ' ORDER BY value) AS joined_list
FROM (
select aa.add_number value from ntaddr aa where e.tnum = aa.tnum and e.primename = aa.primename AND aa.street_id = e.street_id
) distinct_values
        )
        )
    WHEN odd_low = odd_hi and even_low <> even_hi THEN CONCAT(odd_low,' & Even ',even_low, ' to ', even_hi)
    WHEN even_low = even_hi and odd_low <> odd_hi THEN CONCAT(even_low,' & Odd ',odd_low, ' to ', odd_hi)
    WHEN even_low =even_hi and odd_low = odd_hi and odd_low < even_low THEN CONCAT(odd_low,', ',even_low)
    WHEN even_low =even_hi and odd_low = odd_hi and odd_low > even_low THEN CONCAT(even_low,', ',odd_low)
    END AS remark, (SELECT string_agg(value::text, ',' ) AS joined_list
    FROM (
         select case when d.rsn is null or d.rsn = '' then aa.add_number::text
     else concat(aa.add_number::text,'_',d.rsn) end  value
     from ntaddr aa,fst_dnc d where e.tnum = aa.tnum and e.primename = aa.primename and d.addr_id = aa.id
     order by aa.add_number
     ) subquery ) dnc_list
FROM e 
), z as materialized (
   select tnum,street_id,primename,cnt,
        case
        WHEN exists (select 1 from  one_road_tnum o 
            where  f.tnum = o.odd_tnum and f.primename = o.primename and  o.even_tnum = o.odd_tnum ) THEN
        case when f.cnt <= 3 then
        CONCAT('All (',f.remark,')')
        else
        'All'
        end
        else f.remark
        end 
        remark, dnc_list from f
)
SELECT * FROM z;

    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'tnroadi_idx') THEN
CREATE INDEX tnroadi_idx ON tnroadi(primename,tnum);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'fsv_addr2roadside_unique_idx') THEN
CREATE UNIQUE INDEX fsv_addr2roadside_unique_idx ON fsv_addr2roadside (addr_id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'fsv_addr2roadside_road_id_idx') THEN
CREATE INDEX fsv_addr2roadside_road_id_idx ON fsv_addr2roadside (road_id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'fsv_terrroad_idx_spatial') THEN
CREATE INDEX fsv_terrroad_idx_spatial ON fsv_terrroad USING GIST (geom);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'fsv_terrroad_unique_idx') THEN
CREATE UNIQUE INDEX fsv_terrroad_unique_idx ON fsv_terrroad(id,cname);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'fsv_terrroad_primename_idx') THEN
CREATE INDEX fsv_terrroad_primename_idx ON fsv_terrroad(tnum,primename);
   END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'fsv_terrroad_street_id_idx') THEN
CREATE INDEX fsv_terrroad_street_id_idx ON fsv_terrroad(street_id);
   END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_fst_road_primename') THEN
CREATE INDEX idx_fst_road_primename ON fst_road (primename);
   END IF;

create or replace view farcardroads as
with sq as  (SELECT  a.tnum, r.primename, 
         ST_Transform(ST_Envelope(ST_Collect(r.geom)),4326)
         AS geom
  FROM fst_road r
  JOIN (
    SELECT a.tnum, rs.road_id, ROW_NUMBER() OVER (PARTITION BY rs.road_id) AS rn
    FROM ntaddr a,fsv_iw t ,fsv_addr2roadside rs
    where a.tnum = t.tnum and t.scale > 8000
    and rs.addr_id = a.id and a.tnum = t.tnum
  ) a ON r.id = a.road_id
  WHERE a.rn = 1 
  GROUP BY a.tnum, r.primename
  )
  SELECT  a.tnum,  r.id
  FROM fst_road r, (
    SELECT a.tnum, rs.road_id, ROW_NUMBER() OVER (PARTITION BY rs.road_id) AS rn
    FROM ntaddr a,fsv_iw t ,fsv_addr2roadside rs
    where a.tnum = t.tnum 
    and rs.addr_id = a.id and a.tnum = t.tnum
  ) a,sq
  WHERE a.rn = 1  and st_contains(sq.geom,r.geom) and  sq.primename = r.primename and sq.tnum = a.tnum
  GROUP BY a.tnum, r.primename, r.id, r.geom,sq.geom;
  
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE delete_fs_views()
AS $$
DECLARE
  hash_value TEXT;
  property_value TEXT;
BEGIN
    drop materialized view IF EXISTS tnroad, fsv_terrroad cascade;
    IF EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'tnroadi_idx') THEN
       drop index tnroadi_idx;
       drop index fsv_terrroad_unique_idx;
       drop index fsv_terrroad_primename_idx;
       drop index fsv_terrroad_street_id_idx;
       drop index fsv_addr2roadside_unique_idx;
       drop index fsv_addr2roadside_road_id_idx;
       DROP INDEX fsv_terrroad_idx_spatial;
       DROP INDEX idx_fst_road_primename;
    END IF;
END;
$$ LANGUAGE plpgsql;

