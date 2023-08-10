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
    SELECT post_code zip,primename,add_number, addnum_suf, min(id) id FROM fst_addr a
    WHERE post_code IS NOT NULL AND primename IS NOT NULL AND add_number IS NOT NULL
    GROUP BY 1,2,3,4
    HAVING count(*)=1
)
UPDATE fst_dnc d
  SET addr_id =  a.id
  FROM  a
    WHERE d.addr_id IS NULL AND d.zip = a.zip AND d.primename  = a.primename 
        AND (
    CASE WHEN (substring(d.add_number from '^(\d+)$'))::integer 
        = a.add_number THEN 
        TRUE
    ELSE (substring(d.add_number from '^(\d+)'))::integer 
        = a.add_number AND
        (substring(d.add_number from '\s*(\D+)\s*$') = a.addnum_suf)
    END);

CREATE OR REPLACE VIEW fsv_iw AS
WITH ca AS MATERIALIZED (
SELECT
    ca.id,
    st_contains(c.geom,ca.geom) contained,
    c.cardprefix || to_char(ROW_NUMBER() OVER (PARTITION BY c.cardprefix ORDER BY ca.id), 'fm000') tnum,
    ca.rotate,
    ca.scale,
    ca.cardtype,
    ca.geom,
    st_envelope(ca.geom) chgeom,
    ca.locale,
    c.id cname,
    ca.notes
FROM fst_cardatlas ca
JOIN fst_cong c ON st_intersects(c.geom, st_startpoint( st_geometryn(ca.geom,1)))
)
SELECT  ca.*,COALESCE (ca.locale,a.post_comm_list) post_comm_list
FROM ca
LEFT JOIN (
    SELECT ca.id,string_agg(DISTINCT a.post_comm, ' & ' ORDER BY a.post_comm) AS post_comm_list
    FROM ca
    JOIN fst_addr a ON st_contains(ca.geom,a.geom)
    GROUP BY 1
) a ON a.id = ca.id;

DROP TABLE IF EXISTS fst_roads;
CREATE TABLE IF NOT exists fst_roads
(
    id serial PRIMARY KEY NOT NULL,
    primename varchar(50),
    roadclass CHAR(10),
    seqno integer NOT NULL,
    geom Geometry(MultiLineString,4326)
);

CREATE INDEX fst_roads_idx ON fst_roads(primename,roadclass);
CREATE INDEX fst_roads_sidx ON fst_roads USING GIST (geom);

INSERT INTO fst_roads
(primename,seqno,roadclass,geom)
WITH aa AS (
  SELECT primename, roadclass, (ST_Dump(ST_Union(ST_Buffer(r1.geom, 0.00001)))).geom AS geom
  FROM (SELECT ST_Union(geom) AS geom FROM fst_cong) c
  JOIN fst_road r1 ON ST_Intersects(c.geom, r1.geom)
  GROUP BY 1,2
),
a AS (SELECT primename, roadclass, ROW_NUMBER() OVER (PARTITION BY primename,roadclass) AS sequence_number, geom
FROM aa),
merged_roads AS (
  SELECT a.primename, a.roadclass, a.sequence_number,ST_LineMerge(ST_Union(r.geom)) AS geom
  FROM fst_road r,a
    WHERE ST_Contains(a.geom, r.geom) AND COALESCE (a.primename = r.primename,a.primename IS NULL AND r.primename IS NULL) 
  GROUP BY a.primename,a.roadclass,a.sequence_number
)
SELECT primename, sequence_number,roadclass, st_multi(geom) geom
FROM merged_roads;

CREATE MATERIALIZED VIEW fsv_terrroad AS
with a as (
SELECT DISTINCT ON (r.id) r.*, iw.tnum, iw.cname,
iw.rotate,
iw.SCALE
FROM fst_road r
JOIN fsv_iw iw ON (st_intersects(iw.geom, r.geom))
WHERE iw.cardtype = 'S'
ORDER BY r.id,iw.id desc
), b AS (
select * from a
union all
select DISTINCT r.*,
NULL,NULL,
NULL::double precision,
NULL::double precision
from fst_road r
JOIN fst_cong c ON st_intersects(c.geom,r.geom)
where r.id not in (select id from a)
)
SELECT DISTINCT ON (b.id) b.*,c.color FROM b
LEFT JOIN colors c ON c.tnum = b.tnum;


CREATE OR REPLACE VIEW fsv_cardatlas AS
WITH a AS (SELECT  DISTINCT tnum tnum FROM fsv_terrroad ft, fst_cong c where st_intersects(c.geom,ft.geom)  AND 
EXISTS (SELECT 1 FROM fsv_addr2roadside rs,fst_addr a WHERE a.id = rs.addr_id AND st_contains(c.geom,a.geom) AND rs.road_id = ft.id)
and tnum IS NOT NULL
UNION ALL 
SELECT tnum FROM fsv_iw  WHERE cardtype <> 'S'
)
SELECT * FROM fsv_iw WHERE tnum IN (SELECT * FROM a);


CREATE OR REPLACE VIEW fsv_cardatlas_cnt AS
WITH aa AS MATERIALIZED (
    SELECT max(cc.id) cid ,rr.id road_id FROM fst_cardatlas cc, fst_road rr WHERE st_intersects(cc.geom,rr.geom) GROUP BY rr.id
)
SELECT ca.id, COUNT(distinct rs.addr_id) AS count
FROM fst_cardatlas ca
JOIN fst_cong c ON c.active AND st_intersects(c.geom, st_startpoint( st_geometryn(ca.geom,1)))
JOIN aa ON aa.cid = ca.id
JOIN fsv_addr2roadside rs ON rs.road_id = aa.road_id
JOIN fst_addr a ON rs.addr_id = a.id AND a.place_type = 'Residence'
GROUP BY ca.id;

CREATE MATERIALIZED VIEW ntaddr AS
SELECT DISTINCT ON (a.id) iw.tnum,ST_Azimuth(a.geom, ST_ClosestPoint(r.geom, a.geom)) rotate, TRUE contained,
a.*,c.id cname,rs.road_id,d.addr_id IS NOT NULL dnc
FROM fst_cong c
JOIN fst_addr a ON st_intersects(c.geom,a.geom) AND a.place_type = 'Residence'
JOIN fsv_addr2roadside rs ON a.id = rs.addr_id
JOIN fst_road r ON rs.road_id = r.id
JOIN fsv_iw iw ON st_intersects(iw.geom,r.geom) AND c.id = iw.cname
LEFT JOIN fst_dnc d ON d.addr_id = a.id
ORDER BY a.id,iw.id DESC;



CREATE OR REPLACE VIEW webmap AS
WITH a AS (
    SELECT tnum, count(*) cnt FROM ntaddr
    GROUP BY 1
), b AS (
SELECT tnum, ST_CollectionExtract(ST_ConcaveHull(ST_Collect(geom), 0.3), 3) AS geom FROM ntaddr n
WHERE tnum IS NOT NULL
GROUP BY tnum
)
SELECT a.tnum name, a.tnum || ':' || a.cnt description, b.geom FROM a,b
WHERE a.tnum = b.tnum
ORDER BY 1;

create or replace view  fsv_ua as 
SELECT a.* FROM fst_addr a, fst_cong c
WHERE NOT EXISTS (
SELECT 1 FROM fst_road r
JOIN fst_cardatlas ca ON st_intersects(c.geom, st_startpoint( st_geometryn(ca.geom,1))) AND st_intersects(ca.geom,r.geom)
JOIN fsv_addr2roadside rs ON rs.addr_id = a.id AND r.id = rs.road_id
) AND c.active AND st_intersects(c.geom,a.geom) AND a.place_type = 'Residence';

create or replace view fsv_ur as 
select distinct on (r.id)  a.id addr_id,r.* from fst_road r,fsv_addr2roadside  rs, fsv_ua a where r.id = rs.road_id and rs.addr_id = a.id  order by r.id;

CREATE MATERIALIZED VIEW fsv_roadrange AS
WITH a AS (
SELECT cname,tnum,road_id,TRUE iseven, min(add_number) lonum, max(add_number) hinum, count( id) cnt FROM ntaddr  WHERE mod(add_number,2) = 0
GROUP BY 1,2,3
UNION ALL 
SELECT cname,tnum,road_id,FALSE iseven, min(add_number) lonum, max(add_number) hinum, count(id) cnt FROM ntaddr WHERE mod(add_number,2) <> 0
GROUP BY 1,2,3
), b AS (
SELECT ROW_NUMBER () OVER() id,a.cname,a.tnum,a1.primename,a.iseven,a.road_id, a.lonum,a.hinum,a.cnt,
COALESCE ((
SELECT 1 FROM a b,fst_road b1 WHERE a.tnum = b.tnum AND a1.primename = b1.primename AND st_intersects(a1.geom,b1.geom)
AND b.hinum < a.lonum AND a1.id <> b1.id AND b.road_id = b1.id AND a.iseven = b.iseven
LIMIT 1
) <> 1 , TRUE) startrange, FALSE endrange FROM a a, fst_road a1 WHERE a.road_id = a1.id
), c AS (
SELECT cname,tnum,primename,iseven,seqno,min(lonum) lonum,max(hinum) hinum,min(id) id,sum(cnt) cnt FROM (SELECT 
  cname, 
  tnum, 
  primename, 
  iseven, 
  lonum, hinum,id,
  RANK() OVER (ORDER BY tnum,primename,iseven) street_side_id,
  SUM(CASE WHEN startrange THEN 1 ELSE 0 END) 
    OVER (PARTITION BY tnum, primename, iseven ORDER BY lonum) seqno,
  cnt
FROM b) b
GROUP BY 1,2,3,4,5
),
d AS (
SELECT c.*,d.cnt tnumcnt,e.cnt splitcnt FROM c,(SELECT cname,primename,count(DISTINCT tnum) cnt FROM c GROUP BY 1,2) d,
(SELECT cname,tnum,primename,iseven,count(*) cnt FROM c GROUP BY 1,2,3,4) e
WHERE
c.cname = d.cname AND c.primename = d.primename AND
c.primename = e.primename AND c.iseven = e.iseven AND c.tnum = e.tnum
),
e AS (
SELECT DISTINCT cname,tnum, primename,NULL::boolean iseven,id,
(SELECT min(lonum) FROM d e WHERE d.cname = e.cname AND d.tnum = e.tnum AND d.primename = e.primename ) lonum,
(SELECT max(hinum) FROM d e WHERE d.cname = e.cname AND d.tnum = e.tnum AND d.primename = e.primename ) hinum,
(SELECT sum(cnt) FROM d e WHERE d.cname = e.cname AND d.tnum = e.tnum AND d.primename = e.primename ) cnt
FROM d d WHERE tnumcnt = 1
UNION ALL 
SELECT cname,tnum,primename,iseven,id,lonum,hinum,cnt FROM d WHERE tnumcnt <> 1
),dnc AS (
SELECT e.*,ad.add_number,d.rsn FROM e,b a,fst_dnc d,fst_addr ad,fsv_addr2roadside rs
WHERE a.tnum = e.tnum AND a.primename = e.primename  AND (a.iseven = e.iseven AND 
((MOD(ad.add_number,2) = 0) = e.iseven  ) OR (e.iseven IS NULL) AND NOT a.iseven) AND
rs.road_id = a.road_id AND rs.addr_id = ad.id AND ad.add_number BETWEEN e.lonum AND e.hinum AND d.addr_id = ad.id
), dnc_sorted AS (
    SELECT id,
           CASE WHEN rsn <> '' THEN add_number::varchar || '_' || rsn ELSE add_number::varchar END AS sorted_value
    FROM dnc
    ORDER BY id,add_number
)
, dnc2 AS (
    SELECT id, string_agg(sorted_value, ', ') AS dnc_list
    FROM dnc_sorted
    GROUP BY id
)
SELECT DISTINCT e.cname,e.tnum,e.primename,e.iseven,e.lonum,e.hinum,CASE WHEN e.iseven IS NULL THEN 'All ' WHEN  e.iseven THEN 'Even ' ELSE 'Odd ' END ||
 '('|| CASE 
     WHEN e.cnt > 2 THEN 
 (e.lonum || ' to ' ||  e.hinum  )
     WHEN e.cnt = 2 THEN 
 (e.lonum || ', ' ||  e.hinum  )
     WHEN e.cnt = 1 THEN 
 e.lonum || ''  
 END || ')'  rnge,
e.cnt,COALESCE (dnc2.dnc_list,'') dnc_list  FROM e
LEFT JOIN dnc2 ON e.id = dnc2.id;

CREATE OR REPLACE VIEW fsv_roads as
SELECT dense_rank() OVER(ORDER BY geom) id,rr.tnum,COALESCE (rr.primename,CAST('' AS varchar(50))) primename,roadclass,geom FROM fst_roads r
left JOIN (SELECT DISTINCT tnum,primename FROM fsv_roadrange) rr ON r.primename = rr.primename;


CREATE OR REPLACE VIEW fsv_coveratlas AS 
SELECT iw.tnum,
CAST(st_multi(CASE WHEN  ST_GeometryType(ad.geom) = 'ST_Polygon' THEN ad.geom 
ELSE iw.geom END) AS geometry(MultiPolygon,4326)) geom,
iw.rotate,iw.SCALE,iw.cardtype,iw.locale,iw.cname,iw.notes,iw.post_comm_list
,a.str html_table_rows,(SELECT count(*) FROM ntaddr na WHERE na.tnum = iw.tnum) cnt
FROM (SELECT tnum,st_convexhull(st_collect(geom)) geom FROM ntaddr GROUP BY 1) ad
JOIN fsv_iw iw ON iw.tnum = ad.tnum
LEFT JOIN 
(SELECT tnum,
        '<tr>' || string_agg(a.str,'</tr><tr>') || '</tr>' str  
FROM (
SELECT tnum,
             CASE WHEN parity = 1 THEN '<td class="spanned" id="row'|| CASE WHEN MOD(seqno,2) = 0 THEN '1' ELSE '2' END || '" rowspan="'||total_parity||'" '|| 
             'style="text-align: center;"' || '>'||primename || '</td>' ELSE '' end
             ||'<td class="no-wrap'|| CASE WHEN parity = 1 THEN ' spanned' ELSE '' END  ||'" style="text-align: center;">' || cnt || '</td><td class="no-wrap'|| CASE WHEN parity = 1 THEN ' spanned' ELSE '' END  ||'">' 
             || rnge || '</td><td style="text-align: center;" class="'|| CASE WHEN parity = 1 THEN ' spanned' ELSE '' END  ||'" >' || dnc_list || '</td>' str
FROM (
SELECT tnum,
             ROW_NUMBER() OVER(ORDER BY tnum,primename,rnge) seqno,primename,cnt,rnge,dnc_list,
             ROW_NUMBER() OVER(PARTITION BY tnum, primename ) AS  parity,
             COUNT(*) OVER(PARTITION BY tnum,primename) AS total_parity
      FROM fsv_roadrange) a
      ) a
group BY 1) a ON iw.tnum = a.tnum
UNION ALL 
SELECT ca.tnum,ca.geom,ca.rotate,ca.SCALE,ca.cardtype,ca.LOCALe,ca.cname,ca.notes,ca.post_comm_list,   
string_agg('<tr><td style="text-align: center;">'  || b.name || '</td><td style="text-align: center;">'||
COALESCE(b.dnc_list,'')||'</td><tr>','') html_table, NULL::integer cnt
FROM  fsv_cardatlas ca 
LEFT JOIN (SELECT name,dnc_list,geom FROM fst_buildings ORDER BY name) b ON st_intersects(ca.geom,b.geom) AND b.name IS NOT null
WHERE ca.cardtype = 'C'
GROUP BY 1,2,3,4,5,6,7,8,9;

    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'tnroadi_idx') THEN
-- CREATE INDEX tnroadi_idx ON tnroadi(primename,tnum);
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
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'fst_road_sidx') THEN
CREATE INDEX fst_road_sidx ON fst_road USING GIST (geom);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'fst_cardatlas_sidx') THEN
CREATE INDEX fst_cardatlas_sidx ON fst_cardatlas USING GIST (geom);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'fst_addr_place_type_idx') THEN
CREATE INDEX fst_addr_place_type_idx ON fst_addr(place_type);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'fst_addr_sidx') THEN
CREATE INDEX fst_addr_sidx ON fst_addr USING GIST (geom);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'fsv_terrroad_idx') THEN
CREATE INDEX fsv_terrroad_idx ON fsv_terrroad(id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'fst_cardatlas_idx') THEN
CREATE INDEX fst_cardatlas_idx ON fst_cardatlas(id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'fst_addr_primename_idx') THEN
CREATE INDEX fst_addr_primename_idx ON fst_addr(primename);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'fst_road_primename_idx') THEN
CREATE INDEX fst_road_primename_idx ON fst_road(primename);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'fsv_terrroad_primename_idx') THEN
CREATE INDEX fsv_terrroad_primename_idx ON fsv_terrroad(tnum,primename);
   END IF;
   IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_fst_road_primename') THEN
CREATE INDEX idx_fst_road_primename ON fst_road (primename);
   END IF;
   IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'ntaddr_idx') THEN
CREATE UNIQUE INDEX ntaddr_idx ON ntaddr (id);
   END IF;
   IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'fsv_roadrange_idx') THEN
CREATE INDEX fsv_roadrange_idx ON fsv_roadrange(tnum,primename);
   END IF;

create or replace view farcardroads as
with sq as  (SELECT  a.tnum, r.primename, 
         ST_Transform(ST_Envelope(ST_Collect(r.geom)),4326)
         AS geom
  FROM fst_road r
  JOIN (
    SELECT a.tnum, rs.road_id, ROW_NUMBER() OVER (PARTITION BY rs.road_id) AS rn
    FROM ntaddr a,fsv_cardatlas t ,fsv_addr2roadside rs
    where a.tnum = t.tnum and t.scale > 8000
    and rs.addr_id = a.id and a.tnum = t.tnum
  ) a ON r.id = a.road_id
  WHERE a.rn = 1 
  GROUP BY a.tnum, r.primename
  )
  SELECT  a.tnum,  r.id
  FROM fst_road r, (
    SELECT a.tnum, rs.road_id, ROW_NUMBER() OVER (PARTITION BY rs.road_id) AS rn
    FROM ntaddr a,fsv_cardatlas t ,fsv_addr2roadside rs
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
    drop materialized view IF EXISTS tnroad, fsv_terrroad, ntaddr,fsv_roadrange cascade;
    IF EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'tnroadi_idx') THEN
       drop index tnroadi_idx;
       drop index fsv_terrroad_unique_idx;
       drop index fsv_terrroad_primename_idx;
       drop index fsv_addr2roadside_unique_idx;
       drop index fsv_addr2roadside_road_id_idx;
       DROP INDEX fsv_terrroad_idx_spatial;
       DROP INDEX idx_fst_road_primename;
       DROP INDEX fst_cardatlas_idx;
       DROP INDEX ntaddr_idx;
       DROP INDEX fst_addr_primename_idx;
       DROP INDEX fst_road_primename_idx;
       DROP INDEX fst_road_sidx;
       DROP INDEX fst_addr_sidx;
       DROP INDEX fst_addr_place_type_idx;
       DROP INDEX fst_cardatlas_sidx;
       DROP INDEX fsv_roadrange_idx;
    END IF;
END;
$$ LANGUAGE plpgsql;

