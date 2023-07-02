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


create or replace view fsv_iw as
WITH a as (
select row_number() over() id,* from input_wkt where trim(wkt)<>''
),lastrow as ( select max(id)+1 id from a)
,cl as (
select * from  a where regexp_match(wkt,'^[A-Z]$') is not null
union all select lastrow.id,'END' from lastrow
),cr as (
select c1.wkt,min(c1.id)  startid,min(c2.id)-1 endid from cl c1, cl c2
where c1.id < c2.id
group by c1.wkt
)
select
(cr.wkt || to_char(row_number() OVER (PARTITION BY cr.wkt), 'fm000')) tnum,
CASE
WHEN (a.wkt ~ '^\s*-?[0-9.]+\s'::text) THEN ("substring"(TRIM(BOTH FROM a.wkt), '^-?[0-9.]+'::text))::double precision
ELSE NULL::double precision
END AS rotation,
CASE
WHEN (a.wkt ~ '^\s*-?[0-9.]+\s+[0-9.]+\s'::text) THEN ((regexp_split_to_array(TRIM(BOTH FROM a.wkt), '\s+'::text))[2])::double precision
ELSE NULL::double precision
END AS scale,
regexp_replace(
  a.wkt, '^\s*-?\d*\.?\d*\s*-?\d*\.?\d*\s*(.*?)\s*$', '\1') wkt
from a,cr where a.id between cr.startid and cr.endid
and cr.startid <> a.id;

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

CREATE MATERIALIZED VIEW fsv_terrroad AS
with a as (
SELECT DISTINCT ON (r.id) r.*, iw.tnum,
iw.rotation,
iw.SCALE, s.cname,s.id street_id
FROM fst_road r
join fsv_street s on st_intersects(s.geom,r.geom)  AND s.primename = r.primename
JOIN fsv_iw iw ON (st_intersects(ST_GeomFromText(iw.wkt, 4326), r.geom))
WHERE ((iw.rotation IS NULL) OR (iw.rotation >= ('-1000'::integer)::double precision))
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
a.* from fsv_terrroad tr, fsv_addr2roadside rs,fst_addr a ,fst_cong c, fst_road r
where c.active and st_Intersects(c.geom,a.geom) and rs.road_id = tr.id and a.id = rs.addr_id and a.place_type = 'Residence' and r.id = rs.road_id
;

CREATE MATERIALIZED VIEW tnbounds AS
WITH aa AS (
    SELECT
        ntaddr.tnum,
        CASE
            WHEN count(*) = 1 THEN ST_Buffer(ST_Collect(geom), 1) -- Modify the buffer distance as needed
            WHEN count(*) = 2 THEN ST_ConvexHull(ST_Buffer(ST_Collect(geom),1)) -- Modify the buffer distance as needed
            ELSE st_concavehull(ST_Collect(geom), 0.3, false)
        END AS geom,
        count(*) AS cnt
    FROM
        ntaddr
    GROUP BY
        ntaddr.tnum
    
)
SELECT
    rotation AS rotation_angle,scale,
    ST_MakePoint(
        (ST_XMin(ST_Envelope(geom)) + ST_XMax(ST_Envelope(geom))) / 2,
        (ST_YMin(ST_Envelope(geom)) + ST_YMax(ST_Envelope(geom))) / 2
    ) AS midpoint,
    aa.*
FROM
    aa
JOIN (
    SELECT DISTINCT ON (t.tnum) t.tnum, t.rotation, t.scale
    FROM fsv_terrroad t
) tr ON tr.tnum = aa.tnum;

create or replace view fsv_bcomplex  as
select a.tnum,ST_GeomFromText(a.wkt, 4326) geom,
b.* from fsv_iw a, fst_bcomplex b
where rotation <= -1000 and a.rotation::integer = b.id;

create or replace view tnbounds2 as 
WITH const AS (
SELECT (131.649 / 58.149) AS aspect_ratio_viewport
), mbr AS (
SELECT tnbounds.tnum,cnt,rotation_angle user_angle,scale user_scale,
st_orientedenvelope(tnbounds.geom) AS mbr_geom,
st_envelope(st_orientedenvelope(tnbounds.geom)) AS mbre_geom,
tnbounds.geom
FROM tnbounds
), mbr_pts_for_angle AS (
SELECT st_xmin((mbr.mbre_geom)::box3d) AS xmin,
st_xmax((mbr.mbre_geom)::box3d) AS xmax,
st_ymin((mbr.mbre_geom)::box3d) AS ymin,
st_ymax((mbr.mbre_geom)::box3d) AS ymax,
user_angle,user_scale,cnt,
st_pointn(st_exteriorring(mbr.mbr_geom), 1) AS point1,
st_pointn(st_exteriorring(mbr.mbr_geom), 2) AS point2,
st_setsrid(st_makepoint(st_x(st_pointn(st_exteriorring(mbr.mbr_geom), 1)), st_y(st_pointn(st_exteriorring(mbr.mbr_geom), 2))), st_srid(st_pointn(st_exteriorring(mbr.mbr_geom), 1))) AS point3,
(st_ymax((mbr.mbre_geom)::box3d) - st_ymin((mbr.mbre_geom)::box3d)) AS mbre_h,
(st_xmax((mbr.mbre_geom)::box3d) - st_xmin((mbr.mbre_geom)::box3d)) AS mbre_w,
((st_xmax((mbr.mbre_geom)::box3d) - st_xmin((mbr.mbre_geom)::box3d)) / (st_ymax((mbr.mbre_geom)::box3d) - st_ymin((mbr.mbre_geom)::box3d))) AS mbre_ratio,
mbr.tnum,
mbr.mbr_geom,
mbr.mbre_geom,
mbr.geom,
const.aspect_ratio_viewport
FROM mbr,
const
), mbr_angle_aspect AS (
SELECT (st_angle(mbr_pts_for_angle.point1, mbr_pts_for_angle.point2, mbr_pts_for_angle.point3) +
CASE
WHEN (((const.aspect_ratio_viewport >= 1.0) AND ((st_distance(mbr_pts_for_angle.point1, mbr_pts_for_angle.point2) / st_distance(mbr_pts_for_angle.point2, mbr_pts_for_angle.point3)) > (1.0)::double precision))) THEN (0)::double precision
ELSE (pi() / (2.0)::double precision)
END) AS ra,
CASE
WHEN (((const.aspect_ratio_viewport >=1.0) AND ((st_distance(mbr_pts_for_angle.point1, mbr_pts_for_angle.point2) / st_distance(mbr_pts_for_angle.point2, mbr_pts_for_angle.point3)) > (1.0)::double precision))) THEN (st_distance(mbr_pts_for_angle.point1, mbr_pts_for_angle.point2) / st_distance(mbr_pts_for_angle.point2, mbr_pts_for_angle.point3))
ELSE (st_distance(mbr_pts_for_angle.point2, mbr_pts_for_angle.point3) / st_distance(mbr_pts_for_angle.point1, mbr_pts_for_angle.point2))
END AS aspect_ratio,
CASE
WHEN (((const.aspect_ratio_viewport >=1.0) AND ((st_distance(mbr_pts_for_angle.point1, mbr_pts_for_angle.point2) / st_distance(mbr_pts_for_angle.point2, mbr_pts_for_angle.point3)) > (1.0)::double precision))) THEN st_distance(mbr_pts_for_angle.point1, mbr_pts_for_angle.point2)
ELSE st_distance(mbr_pts_for_angle.point2, mbr_pts_for_angle.point3)
END AS mbrer_w,
CASE
WHEN (((const.aspect_ratio_viewport >=1.0) AND ((st_distance(mbr_pts_for_angle.point1, mbr_pts_for_angle.point2) / st_distance(mbr_pts_for_angle.point2, mbr_pts_for_angle.point3)) > (1.0)::double precision))) THEN st_distance(mbr_pts_for_angle.point2, mbr_pts_for_angle.point3)
ELSE st_distance(mbr_pts_for_angle.point1, mbr_pts_for_angle.point2)
END AS mbrer_h,user_angle,user_scale,cnt,
mbr_pts_for_angle.xmin,
mbr_pts_for_angle.xmax,
mbr_pts_for_angle.ymin,
mbr_pts_for_angle.ymax,
mbr_pts_for_angle.point1,
mbr_pts_for_angle.point2,
mbr_pts_for_angle.point3,
mbr_pts_for_angle.mbre_h,
mbr_pts_for_angle.mbre_w,
mbr_pts_for_angle.mbre_ratio,
mbr_pts_for_angle.tnum,
mbr_pts_for_angle.mbr_geom,
mbr_pts_for_angle.mbre_geom,
mbr_pts_for_angle.geom,
mbr_pts_for_angle.aspect_ratio_viewport
FROM mbr_pts_for_angle,
const
), raa AS (
SELECT mbr_angle_aspect.user_angle,mbr_angle_aspect.user_scale,mbr_angle_aspect.cnt,(- degrees((mbr_angle_aspect.ra - (floor((mbr_angle_aspect.ra / (pi() / (2.0)::double precision))) * (pi() / (2.0)::double precision))))) AS rotation_angle,
(degrees((mbr_angle_aspect.ra - (floor((mbr_angle_aspect.ra / (pi() / (2.0)::double precision))) * (pi() / (2.0)::double precision)))) - degrees(atan((const.aspect_ratio_viewport)::double precision))) AS vp_angle,
mbr_angle_aspect.ra,
mbr_angle_aspect.aspect_ratio,
mbr_angle_aspect.mbrer_w,
mbr_angle_aspect.mbrer_h,
mbr_angle_aspect.xmin,
mbr_angle_aspect.xmax,
mbr_angle_aspect.ymin,
mbr_angle_aspect.ymax,
mbr_angle_aspect.point1,
mbr_angle_aspect.point2,
mbr_angle_aspect.point3,
mbr_angle_aspect.mbre_h,
mbr_angle_aspect.mbre_w,
mbr_angle_aspect.mbre_ratio,
mbr_angle_aspect.tnum,
mbr_angle_aspect.mbr_geom,
mbr_angle_aspect.mbre_geom,
mbr_angle_aspect.geom,
mbr_angle_aspect.aspect_ratio_viewport
FROM mbr_angle_aspect,
const
)
SELECT raa.rotation_angle,
raa.vp_angle,
raa.ra,
raa.cnt,
raa.user_angle,
raa.user_scale,
raa.aspect_ratio,
raa.xmin,
raa.xmax,
raa.ymin,
raa.ymax,
raa.mbre_ratio,
raa.tnum,
raa.mbr_geom,
raa.mbre_geom,
raa.geom,
raa.aspect_ratio_viewport
FROM raa;

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
select aa.add_number value from ntaddr aa where e.tnum = aa.tnum and e.primename = aa.primename
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

    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'tnroadi_unique_idx') THEN
CREATE UNIQUE INDEX tnroadi_unique_idx ON tnroadi(primename,tnum);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'fsv_addr2roadside_unique_idx') THEN
CREATE UNIQUE INDEX fsv_addr2roadside_unique_idx ON fsv_addr2roadside (addr_id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'tnbounds_unique_idx') THEN
CREATE UNIQUE INDEX tnbounds_unique_idx ON tnbounds(tnum);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'fsv_addr2roadside_road_id_idx') THEN
CREATE INDEX fsv_addr2roadside_road_id_idx ON fsv_addr2roadside (road_id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'tnbounds_idx_spatial') THEN
CREATE INDEX tnbounds_idx_spatial ON tnbounds USING GIST (geom);
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

CREATE OR REPLACE VIEW ttown AS
WITH ttown_agg AS (
    SELECT tnum, string_agg(post_comm, ' & ' ORDER BY post_comm) AS post_comm_list
    FROM (
        SELECT DISTINCT ON (tnum, post_comm) tnum, post_comm
        FROM ntaddr
        ORDER BY tnum, post_comm
    ) subquery
    GROUP BY tnum
)
SELECT *
FROM ttown_agg;

create or replace view farcardroads as
with sq as  (SELECT  a.tnum, r.primename, 
         ST_Transform(ST_Envelope(ST_Collect(r.geom)),4326)
         AS geom
  FROM fst_road r
  JOIN (
    SELECT a.tnum, rs.road_id, ROW_NUMBER() OVER (PARTITION BY rs.road_id) AS rn
    FROM ntaddr a,tnbounds2 t ,fsv_addr2roadside rs
    where a.tnum = t.tnum and t.user_scale > 8000
    and rs.addr_id = a.id and a.tnum = t.tnum
  ) a ON r.id = a.road_id
  WHERE a.rn = 1 
  GROUP BY a.tnum, r.primename
  )
  SELECT  a.tnum,  r.id
  FROM fst_road r, (
    SELECT a.tnum, rs.road_id, ROW_NUMBER() OVER (PARTITION BY rs.road_id) AS rn
    FROM ntaddr a,tnbounds2 t ,fsv_addr2roadside rs
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
    drop materialized view IF EXISTS tnbounds,tnroad, fsv_terrroad cascade;
    IF EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'tnroadi_unique_idx') THEN
       drop index tnroadi_unique_idx;
       drop index fsv_terrroad_unique_idx;
       drop index fsv_terrroad_primename_idx;
       drop index fsv_terrroad_street_id_idx;
       drop index fsv_addr2roadside_unique_idx;
       drop index fsv_addr2roadside_road_id_idx;
       drop index tnbounds_idx_spatial;
       drop index tnbounds_unique_idx;
       DROP INDEX fsv_terrroad_idx_spatial;
    END IF;
END;
$$ LANGUAGE plpgsql;

