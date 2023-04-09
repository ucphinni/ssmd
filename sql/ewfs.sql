CREATE TABLE fst_dnc (
  id SERIAL PRIMARY KEY,
  tnum VARCHAR(4),
  address TEXT,
  addr_id INTEGER,
  FOREIGN KEY (addr_id) REFERENCES fst_addr(id),
  FOREIGN KEY (tnum) REFERENCES fst_card(name)
);

CREATE TABLE fst_work (
  id SERIAL PRIMARY KEY,
  tnum VARCHAR(4),
  hnum_start INTEGER,
  hnum_end INTEGER,
  addr_primename TEXT,
  FOREIGN KEY (tnum) REFERENCES fst_card(name)  
);
