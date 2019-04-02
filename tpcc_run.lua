#!/usr/bin/env sysbench

-- This program is free software; you can redistribute it and/or modify
-- it under the terms of the GNU General Public License as published by
-- the Free Software Foundation; either version 2 of the License, or
-- (at your option) any later version.

-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.

-- You should have received a copy of the GNU General Public License
-- along with this program; if not, write to the Free Software
-- Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

-- ----------------------------------------------------------------------
-- TPCC-like workload
-- ----------------------------------------------------------------------

require("tpcc_common")


--
-- produce the id of a valid warehouse other than home_ware
-- (assuming there is one)
--
function other_ware (home_ware)
    local tmp

    if sysbench.opt.scale == 1 then return home_ware end
    repeat
       tmp = sysbench.rand.uniform(1, sysbench.opt.scale)
    until tmp == home_ware
    return tmp
end
-- mongo?
function new_order()

-- prep work

    local table_num = sysbench.rand.uniform(1, sysbench.opt.tables)
    local w_id = sysbench.rand.uniform(1, sysbench.opt.scale)
    local d_id = sysbench.rand.uniform(1, DIST_PER_WARE)
    local c_id = NURand(1023, 1, CUST_PER_DIST)

    local ol_cnt = sysbench.rand.uniform(5, 15);
    local rbk = sysbench.rand.uniform(1, 100);
    local itemid = {}
    local supware = {}
    local qty = {}
    local all_local = 1

    for i = 1, ol_cnt
    do
        itemid[i] = NURand(8191, 1, MAXITEMS)
        if ((i == ol_cnt - 1) and (rbk == 1))
	then
            itemid[i] = -1
        end
        if sysbench.rand.uniform(1, 100) ~= 1
	then
            supware[i] = w_id
        else 
            supware[i] = other_ware(w_id)
            all_local = 0
        end
        qty[i] = sysbench.rand.uniform(1, 10)
   end


--  SELECT c_discount, c_last, c_credit, w_tax
--  INTO :c_discount, :c_last, :c_credit, :w_tax
--  FROM customer, warehouse
--  WHERE w_id = :w_id 
--  AND c_w_id = w_id 
--  AND c_d_id = :d_id 
--  AND c_id = :c_id;

  con:query("BEGIN")

  local c_discount
  local c_last
  local c_credit
  local w_tax

  if (drv:name() ~= "mongodb") 
  then
	c_discount, c_last, c_credit, w_tax = con:query_row(([[SELECT c_discount, c_last, c_credit, w_tax 
                                                           FROM customer%d, warehouse%d
                                                          WHERE w_id = %d 
                                                            AND c_w_id = w_id 
                                                            AND c_d_id = %d 
                                                            AND c_id = %d]]):
                                                         format(table_num, table_num, w_id, d_id, c_id))
  else
  -- mongo？多表查询
	c_discount, c_last, c_credit, w_tax = con:query_row(([[db.customer%d.find]]):format(table_num, table_num, w_id, d_id, c_id)
  ))

  end
--        SELECT d_next_o_id, d_tax INTO :d_next_o_id, :d_tax
--                FROM district
--                WHERE d_id = :d_id
--                AND d_w_id = :w_id
--                FOR UPDATE
  local d_next_o_id
  local d_tax
  if (drv:name() ~= "mongodb") 
  then
	d_next_o_id, d_tax = con:query_row(([[SELECT d_next_o_id, d_tax 
                                          FROM district%d 
                                         WHERE d_w_id = %d 
                                           AND d_id = %d FOR UPDATE]]):
                                        format(table_num, w_id, d_id))
  else
    d_next_o_id, d_tax = con:query_row(([[db.district%d.find({d_w_id:%d,d_id:%d,upsert:true},{d_next_o_id:1,d_tax:1} )]]):
                                        format(table_num, w_id, d_id))
  end

-- UPDATE district SET d_next_o_id = :d_next_o_id + 1
--                WHERE d_id = :d_id 
--                AND d_w_id = :w_id;
  if (drv:name() ~= "mongodb") 
  then
    con:query(([[UPDATE district%d
                  SET d_next_o_id = %d
                WHERE d_id = %d AND d_w_id= %d]]):format(table_num, d_next_o_id + 1, d_id, w_id))
  else
        con:query(([[db.district%d.updateMany({d_id:%d,d_w_id:%d},{$set:{d_next_o_id:%d}})]]):format(table_num, d_id, w_id, d_next_o_id + 1))
  end
--INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id,
--                                    o_entry_d, o_ol_cnt, o_all_local)
--                VALUES(:o_id, :d_id, :w_id, :c_id, 
--                       :datetime,
--                       :o_ol_cnt, :o_all_local);
  if (drv:name() ~= "mongodb") 
  then
    con:query(([[INSERT INTO orders%d
                           (o_id, o_d_id, o_w_id, o_c_id,  o_entry_d, o_ol_cnt, o_all_local)
                    VALUES (%d,%d,%d,%d,NOW(),%d,%d)]]):
                    format(table_num, d_next_o_id, d_id, w_id, c_id, ol_cnt, all_local))
  else
     con:query(([[db.orders%d.insertOne({o_id:%d,o_d_id:%d,o_w_id:%d,o_c_id:%d,o_entry_d:%d,o_ol_cnt:%d,o_all_local:%d})]]):
                    format(table_num, d_next_o_id, d_id, w_id, c_id, ol_cnt, all_local))
  end
-- INSERT INTO new_orders (no_o_id, no_d_id, no_w_id)
--    VALUES (:o_id,:d_id,:w_id); */
  if (drv:name() ~= "mongodb") 
  then
    con:query(([[INSERT INTO new_orders%d (no_o_id, no_d_id, no_w_id)
                    VALUES (%d,%d,%d)]]):
                   format(table_num, d_next_o_id, d_id, w_id))
  else
     con:query(([[db.new_orders%d.insertOne({no_o_id:%d, no_d_id:%d, no_w_id:%d})]]):
                   format(table_num, d_next_o_id, d_id, w_id))
  end
  for ol_number=1, ol_cnt do
	local ol_supply_w_id = supware[ol_number]
	local ol_i_id = itemid[ol_number]
	local ol_quantity = qty[ol_number]

-- SELECT i_price, i_name, i_data
--	INTO :i_price, :i_name, :i_data
--	FROM item
--	WHERE i_id = :ol_i_id;*/
    if (drv:name() ~= "mongodb") 
    then
	 rs = con:query(([[SELECT i_price, i_name, i_data 
	                    FROM item%d
	                   WHERE i_id = %d]]):
	                  format(table_num, ol_i_id))
    else
      rs = con:query(([[db.item%d.find({i_id:%d},{ i_price:1, i_name:1, i_data:1 })]]):
	                  format(table_num, ol_i_id))
    end
	local i_price
	local i_name
	local i_data

	if rs.nrows == 0 then
--          print("ROLLBACK")
          ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_ERROR)
          con:query("ROLLBACK")
	  return	
        end
        
        i_price, i_name, i_data = unpack(rs:fetch_row(), 1, rs.nfields)
        
-- SELECT s_quantity, s_data, s_dist_01, s_dist_02,
--		s_dist_03, s_dist_04, s_dist_05, s_dist_06,
--		s_dist_07, s_dist_08, s_dist_09, s_dist_10
--	INTO :s_quantity, :s_data, :s_dist_01, :s_dist_02,
--	     :s_dist_03, :s_dist_04, :s_dist_05, :s_dist_06,
--	     :s_dist_07, :s_dist_08, :s_dist_09, :s_dist_10
--	FROM stock
--	WHERE s_i_id = :ol_i_id 
--	AND s_w_id = :ol_supply_w_id
--	FOR UPDATE;*/

        local s_quantity 
        local s_data 
        local ol_dist_info
    if (drv:name() ~= "mongodb") 
    then
	s_quantity, s_data, ol_dist_info = con:query_row(([[SELECT s_quantity, s_data, s_dist_%s s_dist 
	                                                      FROM stock%d  
	                                                     WHERE s_i_id = %d AND s_w_id= %d FOR UPDATE]]):
	                                                     format(string.format("%02d",d_id),table_num,ol_i_id,ol_supply_w_id ))
    else
		s_quantity, s_data, ol_dist_info = con:query_row(([[db.stock%d.findAndModify({ s_i_id:%d,s_w_id:%d},{s_quantity:1, s_data:1,s_dist_%s:1, s_dist:1}) ]] ):
	                                                     format(table_num,ol_i_id,ol_supply_w_id,string.format("%02d",d_id)))
	end
        s_quantity=tonumber(s_quantity)
  	if (s_quantity > ol_quantity) then
	        s_quantity = s_quantity - ol_quantity
	else
		s_quantity = s_quantity - ol_quantity + 91
	end

-- UPDATE stock SET s_quantity = :s_quantity
--	WHERE s_i_id = :ol_i_id 
--	AND s_w_id = :ol_supply_w_id;*/
    if (drv:name() ~= "mongodb") 
    then
	con:query(([[UPDATE stock%d
	                SET s_quantity = %d
	              WHERE s_i_id = %d 
		        AND s_w_id= %d]]):
		    format(table_num, s_quantity, ol_i_id, ol_supply_w_id))
   
        i_price=tonumber(i_price)
        w_tax=tonumber(w_tax)
        d_tax=tonumber(d_tax)        
        c_discount=tonumber(c_discount)
    else
	con:query(([[db.stock%d.update({$set,{s_quantity:%d}},{s_i_id:%d,a_w_id:%d})]]):
		    format(table_num, s_quantity, ol_i_id, ol_supply_w_id))
   
        i_price=tonumber(i_price)
        w_tax=tonumber(w_tax)
        d_tax=tonumber(d_tax)        
        c_discount=tonumber(c_discount)
	end
	ol_amount = ol_quantity * i_price * (1 + w_tax + d_tax) * (1 - c_discount);

-- INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, 
--				 ol_number, ol_i_id, 
--				 ol_supply_w_id, ol_quantity, 
--				 ol_amount, ol_dist_info)
--	VALUES (:o_id, :d_id, :w_id, :ol_number, :ol_i_id,
--		:ol_supply_w_id, :ol_quantity, :ol_amount,
--		:ol_dist_info);
    if (drv:name() ~= "mongodb") 
    then
	con:query(([[INSERT INTO order_line%d
                                 (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)
	                  VALUES (%d,%d,%d,%d,%d,%d,%d,%d,'%s')]]):
	                  format(table_num, d_next_o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info))
    else
	con:query(([[db.order_line%d.insertOne({ol_o_id:%d, ol_d_id:%d, ol_w_id:%d, ol_number:%d, ol_i_id:%d, ol_supply_w_id:%d, ol_quantity:%d, ol_amount:%d, ol_dist_info:'%s'})]]):
	                  format(table_num, d_next_o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info))
    end
  end

  con:query("COMMIT")

end

function payment()
-- prep work

    local table_num = sysbench.rand.uniform(1, sysbench.opt.tables)
    local w_id = sysbench.rand.uniform(1, sysbench.opt.scale)
    local d_id = sysbench.rand.uniform(1, DIST_PER_WARE)
    local c_id = NURand(1023, 1, CUST_PER_DIST)
    local h_amount = sysbench.rand.uniform(1,5000)
    local byname
    local c_w_id
    local c_d_id
    local c_last = Lastname(NURand(255,0,999))

    if sysbench.rand.uniform(1, 100) <= 60 then
        byname = 1 -- select by last name 
    else
        byname = 0 -- select by customer id 
    end

    if sysbench.rand.uniform(1, 100) <= 85 then
        c_w_id = w_id
        c_d_id = d_id
    else
        c_w_id = other_ware(w_id)
        c_d_id = sysbench.rand.uniform(1, DIST_PER_WARE)
    end

--  UPDATE warehouse SET w_ytd = w_ytd + :h_amount
--  WHERE w_id =:w_id

  con:query("BEGIN")
  if (drv:name() ~= "mongodb") 
  then
  con:query(([[UPDATE warehouse%d
	          SET w_ytd = w_ytd + %d 
	        WHERE w_id = %d]]):format(table_num, h_amount, w_id ))
  else
    con:query(([[db.warehouse%d.update({$set:{w_ytd:$w_ytd+%d},{w_id:%d}})]]):format(table_num, h_amount, w_id ))
  end


-- SELECT w_street_1, w_street_2, w_city, w_state, w_zip,
--		w_name
--		INTO :w_street_1, :w_street_2, :w_city, :w_state,
--			:w_zip, :w_name
--		FROM warehouse
--		WHERE w_id = :w_id;*/
  local w_street_1, w_street_2, w_city, w_state, w_zip, w_name
  if (drv:name() ~= "mongodb") 
  then
  w_street_1, w_street_2, w_city, w_state, w_zip, w_name =
                          con:query_row(([[SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name 
                                             FROM warehouse%d  
                                            WHERE w_id = %d]]):format(table_num, w_id))
  else
  w_street_1, w_street_2, w_city, w_state, w_zip, w_name =
                          con:query_row(([[db.warehose%d.find({w_id:%d},
						  {w_street_1:1, w_street_2:1, w_city:1, w_state:1, w_zip:1, w_name:1 })]]):format(table_num, w_id))
  end
-- UPDATE district SET d_ytd = d_ytd + :h_amount
--		WHERE d_w_id = :w_id 
--		AND d_id = :d_id;*/
  if (drv:name() ~= "mongodb") 
  then
  con:query(([[UPDATE district%d 
                 SET d_ytd = d_ytd + %d 
               WHERE d_w_id = %d 
                 AND d_id= %d]]):format(table_num, h_amount, w_id, d_id))

  else
    con:query(([[db.district%d.updateMany({$set:{d_ytd:$d_ytd+%d},
				{d_w_id:%d,d_id:%d}})]]):format(table_num, h_amount, w_id, d_id))
  end
  local d_street_1,d_street_2, d_city, d_state, d_zip, d_name

  if (drv:name() ~= "mongodb") 
  then
  d_street_1,d_street_2, d_city, d_state, d_zip, d_name = 
                          con:query_row(([[SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name 
                                             FROM district%d
                                            WHERE d_w_id = %d 
                                              AND d_id = %d]]):format(table_num, w_id, d_id ))
  else 
  d_street_1,d_street_2, d_city, d_state, d_zip, d_name = 
                          con:query_row(([[db.district%d.find({d_w_id:%d,d_id:%d},
						  {d_street_1:1, d_street_2:1, d_city:1, d_state:1, d_zip, d_name:1 })]]):format(table_num, w_id, d_id ))
  end
  if byname == 1 then

-- SELECT count(c_id) 
--	FROM customer
--	WHERE c_w_id = :c_w_id
--	AND c_d_id = :c_d_id
--	AND c_last = :c_last;*/
  if (drv:name() ~= "mongodb") 
  then
	local namecnt = con:query_row(([[SELECT count(c_id) namecnt
			                   FROM customer%d
			                  WHERE c_w_id = %d 
			                    AND c_d_id= %d
                                            AND c_last='%s']]):format(table_num, w_id, c_d_id, c_last ))
  else
    local namecnt = con:query_row(([[db.customer%d.find({c_id:{$exists:true},{c_w_id:%d,c_d_id:%d,c_last:'%s'}}).count()]]):format(table_num, w_id, c_d_id, c_last ))
  end
--		SELECT c_id
--		FROM customer
--		WHERE c_w_id = :c_w_id 
--		AND c_d_id = :c_d_id 
--		AND c_last = :c_last
--		ORDER BY c_first;

	if namecnt % 2 == 0 then
		namecnt = namecnt + 1
	end
  if (drv:name() ~= "mongodb") 
  then
	rs = con:query(([[SELECT c_id
		 	    FROM customer%d
			   WHERE c_w_id = %d AND c_d_id= %d
                             AND c_last='%s' ORDER BY c_first]]
			):format(table_num, w_id, c_d_id, c_last ))
  else
  	rs = con:query(([[db.customer%d.find({c_w_id:%d,c_d_id:%d},{c_id:1})]]
			):format(table_num, w_id, c_d_id, c_last ))
  end
	for i = 1,  (namecnt / 2 ) + 1 do
		row = rs:fetch_row()
		c_id = row[1]
	end
  end -- byname

-- SELECT c_first, c_middle, c_last, c_street_1,
--		c_street_2, c_city, c_state, c_zip, c_phone,
--		c_credit, c_credit_lim, c_discount, c_balance,
--		c_since
--	FROM customer
--	WHERE c_w_id = :c_w_id 
--	AND c_d_id = :c_d_id 
--	AND c_id = :c_id
--	FOR UPDATE;

  local c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip,
        c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_since
  if (drv:name() ~= "mongodb") 
  then
  c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip,
  c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_since =
	 con:query_row(([[SELECT c_first, c_middle, c_last, c_street_1,
                                 c_street_2, c_city, c_state, c_zip, c_phone,
                                 c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_since
			    FROM customer%d
			   WHERE c_w_id = %d 
			     AND c_d_id= %d
			     AND c_id=%d FOR UPDATE]])
			 :format(table_num, w_id, c_d_id, c_id ))
  else
   c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip,
   c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_since =
	 con:query_row(([[db.customer%d.find({c_w_id:%d,c_d_id:%d,c_id:%d},{c_first:1, c_middle:1, c_last:1, c_street_1:1,
                                 c_street_2:1, c_city:1, c_state:1, c_zip:1, c_phone:1,
                                 c_credit:1, c_credit_lim:1, c_discount:1, c_balance:1, c_ytd_payment:1, c_since:1}) ]])
			 :format(table_num, w_id, c_d_id, c_id ))
  end
  c_balance = tonumber(c_balance) - h_amount
  c_ytd_payment = tonumber(c_ytd_payment) + h_amount

  if c_credit == "BC" then
-- SELECT c_data 
--	INTO :c_data
--	FROM customer
--	WHERE c_w_id = :c_w_id 
--	AND c_d_id = :c_d_id 
-- 	AND c_id = :c_id; */

        local c_data
    if (drv:name() ~= "mongodb") 
    then
        c_data = con:query_row(([[SELECT c_data
                                    FROM customer%d
                                   WHERE c_w_id = %d 
                                     AND c_d_id=%d
                                     AND c_id= %d]]):
                                  format(table_num, w_id, c_d_id, c_id ))
    else 
	  c_data = con:query_row(([[db.customer%d.find({c_w_id:%d,c_d_id:%d,c_id:%d},{c_data:1}) ]]):
                                  format(table_num, w_id, c_d_id, c_id ))
    end
        local c_new_data=string.sub(string.format("| %4d %2d %4d %2d %4d $%7.2f %12s %24s",
                c_id, c_d_id, c_w_id, d_id, w_id, h_amount, os.time(), c_data), 1, 500);

    --		UPDATE customer
    --			SET c_balance = :c_balance, c_data = :c_new_data
    --			WHERE c_w_id = :c_w_id 
    --			AND c_d_id = :c_d_id 
    --			AND c_id = :c_id
	if (drv:name() ~= "mongodb") 
    then
        con:query(([[UPDATE customer%d
                        SET c_balance=%f, c_ytd_payment=%f, c_data='%s'
                      WHERE c_w_id = %d 
                        AND c_d_id=%d
                        AND c_id=%d]])
		  :format(table_num, c_balance, c_ytd_payment, c_new_data, w_id, c_d_id, c_id  ))
    else 
	  con:query(([[db.customer%d.updateMany({$set:{c:%f,c_ytd_payment:%f,c_data:'%s'}},{c_w_id:%d,c_d_id:%d,c_id:%d}) ]]):
                                  format(table_num,c_balance, c_ytd_payment, c_new_data, w_id, c_d_id, c_id ))
    end
  else
  	if (drv:name() ~= "mongodb") 
    then
        con:query(([[UPDATE customer%d
                        SET c_balance=%f, c_ytd_payment=%f
                      WHERE c_w_id = %d 
                        AND c_d_id=%d
                        AND c_id=%d]])
		  :format(table_num, c_balance, c_ytd_payment, w_id, c_d_id, c_id  ))
    else
	  con:query(([[db.customer%d.updateMany({$set:{c:%f,c_ytd_payment:%f}},{c_w_id:%d,c_d_id:%d,c_id:%d}) ]]):
                                  format(table_num,c_balance, c_ytd_payment, w_id, c_d_id, c_id ))
	end
  end

--	INSERT INTO history(h_c_d_id, h_c_w_id, h_c_id, h_d_id,
--			                   h_w_id, h_date, h_amount, h_data)
--	                VALUES(:c_d_id, :c_w_id, :c_id, :d_id,
--		               :w_id, 
--			       :datetime,
--			       :h_amount, :h_data);*/
  if (drv:name() ~= "mongodb") 
  then			       
  con:query(([[INSERT INTO history%d
                           (h_c_d_id, h_c_w_id, h_c_id, h_d_id,  h_w_id, h_date, h_amount, h_data)
                    VALUES (%d,%d,%d,%d,%d,NOW(),%d,'%s')]])
            :format(table_num, c_d_id, c_w_id, c_id, d_id,  w_id, h_amount, string.format("%10s %10s    ",w_name,d_name)))

  con:query("COMMIT")
  else
  con:query(([[db.history%d.insertOne({h_c_d_id:%d, h_c_w_id:%d, h_c_id:%d, h_d_id:%d,  h_w_id:%d, h_date:NOW(), h_amount:%d, h_data:'%s'}) ]])
            :format(table_num, c_d_id, c_w_id, c_id, d_id,  w_id, h_amount, string.format("%10s %10s    ",w_name,d_name)))
  end

end
-- mongo?
function orderstatus()

    local table_num = sysbench.rand.uniform(1, sysbench.opt.tables)
    local w_id = sysbench.rand.uniform(1, sysbench.opt.scale)
    local d_id = sysbench.rand.uniform(1, DIST_PER_WARE)
    local c_id = NURand(1023, 1, CUST_PER_DIST)
    local byname
    local c_last = Lastname(NURand(255,0,999))

    if sysbench.rand.uniform(1, 100) <= 60 then
        byname = 1 -- select by last name 
    else
        byname = 0 -- select by customer id 
    end

    local c_balance
    local c_first
    local c_middle
    con:query("BEGIN")

    if byname == 1 then
--    /*EXEC_SQL SELECT count(c_id)
--            FROM customer
--        WHERE c_w_id = :c_w_id
--        AND c_d_id = :c_d_id
--            AND c_last = :c_last;*/

        local namecnt
    if (drv:name() ~= "mongodb") 
    then	
        namecnt = con:query_row(([[SELECT count(c_id) namecnt
                                     FROM customer%d
                                    WHERE c_w_id = %d 
                                      AND c_d_id= %d
                                      AND c_last='%s']]):
                                  format(table_num, w_id, d_id, c_last ))
    else
	    namecnt = con:query_row(([[db.customer%d.find({c_w_id:%d,c_d_id:%d,c_last:'%s'},{c_id:1}).count()]]):
    end                              format(table_num, w_id, d_id, c_last ))
    	
--            SELECT c_balance, c_first, c_middle, c_id
--            FROM customer
--            WHERE c_w_id = :c_w_id
--        AND c_d_id = :c_d_id
--        AND c_last = :c_last
--        ORDER BY c_first;
    if (drv:name() ~= "mongodb") 
    then
        rs = con:query(([[SELECT c_balance, c_first, c_middle, c_id
                            FROM customer%d
                	   WHERE c_w_id = %d 
                  	     AND c_d_id= %d
                             AND c_last='%s' ORDER BY c_first]])
		:format(table_num, w_id, d_id, c_last ))
    else 
	    rs = con:query(([[db.customer%d.find({c_w_id:%d,c_d_id:%d},
		{c_balance:1, c_first:1, c_middle:1, c_id:1}).sort({c_first:1})]])
		:format(table_num, w_id, d_id, c_last ))
	end
        if namecnt % 2 == 0 then
            namecnt = namecnt + 1
        end
        for i = 1,  (namecnt / 2 ) + 1 do
            row = rs:fetch_row()
            c_balance = row[1]
            c_first = row[2]
            c_middle = row[3]
            c_id = row[4]
        end
    else
--		SELECT c_balance, c_first, c_middle, c_last
--		        FROM customer
--		        WHERE c_w_id = :c_w_id
--			AND c_d_id = :c_d_id
--			AND c_id = :c_id;*/
      if (drv:name() ~= "mongodb") 
      then
        c_balance, c_first, c_middle, c_last = 
                   con:query_row(([[SELECT c_balance, c_first, c_middle, c_last
                                      FROM customer%d
                   	             WHERE c_w_id = %d 
                   	               AND c_d_id=%d
                                       AND c_id=%d]])
                                  :format(table_num, w_id, d_id, c_id ))
	  else 
	          c_balance, c_first, c_middle, c_last = 
                   con:query_row(([[db.customer%d.find({c_w_id:%d,c_d_id:%d},{c_balance:1, c_first:1, c_middle:1, c_last:1})]])
                                  :format(table_num, w_id, d_id, c_id ))
	  end
    end
--[=[ Initial query
        SELECT o_id, o_entry_d, COALESCE(o_carrier_id,0) FROM orders 
        WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? AND o_id = 
        (SELECT MAX(o_id) FROM orders WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ?)

        rs = con:query(([[SELECT o_id, o_entry_d, COALESCE(o_carrier_id,0) 
                  FROM orders%d WHERE o_w_id = %d AND o_d_id = %d AND o_c_id = %d AND o_id = 
                  (SELECT MAX(o_id) FROM orders%d WHERE o_w_id = %d AND o_d_id = %d AND o_c_id = %d)]])
                  :format(table_num, w_id, d_id, c_id, table_num, w_id, d_id, c_id)) 
--]=]
                  
--[[ Query from tpcc standard

  EXEC SQL SELECT o_id, o_carrier_id, o_entry_d
  INTO :o_id, :o_carrier_id, :entdate
  FROM orders
  ORDER BY o_id DESC;
-]]
      local o_id
	  if (drv:name() ~= "mongodb") 
      then
      o_id = con:query_row(([[SELECT o_id, o_carrier_id, o_entry_d
                                FROM orders%d 
                               WHERE o_w_id = %d 
                                 AND o_d_id = %d 
                                 AND o_c_id = %d 
                                  ORDER BY o_id DESC]]):
                             format(table_num, w_id, d_id, c_id))
	  else
	        o_id = con:query_row(([[db.orders%d.find({o_w_id:%d,o_d_id:%d,o_c_id:%d},
			{o_id:1, o_carrier_id:1, o_entry_d:1}).sort({o_id:-1})]]):
                             format(table_num, w_id, d_id, c_id))
	  end
--      rs = con:query(([[SELECT o_id, o_carrier_id, o_entry_d
--                                FROM orders%d 
--                              WHERE o_w_id = %d 
--                                 AND o_d_id = %d 
--                                 AND o_c_id = %d 
--                                  ORDER BY o_id DESC]]):
--                             format(table_num, w_id, d_id, c_id))
--     if rs.nrows == 0 then
--	print(string.format("Error o_id %d, %d, %d, %d\n", table_num , w_id , d_id , c_id))
--     end
--    for i = 1,  rs.nrows do
--        row = rs:fetch_row()
--	o_id= row[1] 
--    end

--		SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount,
--                       ol_delivery_d
--		FROM order_line
--	        WHERE ol_w_id = :c_w_id
--		AND ol_d_id = :c_d_id
--		AND ol_o_id = :o_id;*/
	if (drv:name() ~= "mongodb") 
    then
    rs = con:query(([[SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d
            FROM order_line%d WHERE ol_w_id = %d AND ol_d_id = %d  AND ol_o_id = %d]])
                  :format(table_num, w_id, d_id, d_id, o_id))
	else
	    rs = con:query(([[db.order_line%d.find({ol_w_id:%d,ol_d_id:%d,ol_o_id:%d},
		{ ol_i_id:1, ol_supply_w_id:, ol_quantity:, ol_amount:, ol_delivery_d:})]])
                  :format(table_num, w_id, d_id, d_id, o_id))
	end
    for i = 1,  rs.nrows do
        row = rs:fetch_row()
        local ol_i_id = row[1]
        local ol_supply_w_id = row[2]
        local ol_quantity = row[3]
        local ol_amount = row[4]
        local ol_delivery_d = row[5]
    end
    con:query("COMMIT")

end

function delivery()
    local table_num = sysbench.rand.uniform(1, sysbench.opt.tables)
    local w_id = sysbench.rand.uniform(1, sysbench.opt.scale)
    local o_carrier_id = sysbench.rand.uniform(1, 10)

    con:query("BEGIN")
    for  d_id = 1, DIST_PER_WARE do

--	SELECT COALESCE(MIN(no_o_id),0) INTO :no_o_id
--		                FROM new_orders
--		                WHERE no_d_id = :d_id AND no_w_id = :w_id;*/
		                
--        rs = con:query(([[SELECT COALESCE(MIN(no_o_id),0) no_o_id
--                 FROM new_orders%d WHERE no_d_id = %d AND no_w_id = %d FOR UPDATE]])
--                      :format(table_num, d_id, w_id))

        local no_o_id
    if (drv:name() ~= "mongodb") 
    then
        rs = con:query(([[SELECT no_o_id
                                     FROM new_orders%d 
                                    WHERE no_d_id = %d 
                                      AND no_w_id = %d 
                                      ORDER BY no_o_id ASC LIMIT 1 FOR UPDATE]])
                                   :format(table_num, d_id, w_id))
	else
	   rs = con:query(([[db.new_orders%d.findAndModify({no_d_id:%d,no_w_id:%d},{no_o_id:1}).sort({no_o_id:1}).limit(1)]])
                                   :format(table_num, d_id, w_id))
	end
        if (rs.nrows > 0) then
          no_o_id=unpack(rs:fetch_row(), 1, rs.nfields)
        end

        if (no_o_id ~= nil ) then 
        
--		DELETE FROM new_orders WHERE no_o_id = :no_o_id AND no_d_id = :d_id
--		  AND no_w_id = :w_id;*/
    if (drv:name() ~= "mongodb") 
    then
        con:query(([[DELETE FROM new_orders%d
                           WHERE no_o_id = %d 
                             AND no_d_id = %d  
                             AND no_w_id = %d]])
                            :format(table_num, no_o_id, d_id, w_id))
    else
	     con:query(([[db.new_orders%d.deleteMany({no_o_id:%d,no_d_id:%d,no_w_id:%d})]])
                            :format(table_num, no_o_id, d_id, w_id))
	end
--  SELECT o_c_id INTO :c_id FROM orders
--		                WHERE o_id = :no_o_id AND o_d_id = :d_id
--				AND o_w_id = :w_id;*/

        local o_c_id
    if (drv:name() ~= "mongodb") 
    then
        o_c_id = con:query_row(([[SELECT o_c_id
                                    FROM orders%d 
                                   WHERE o_id = %d 
                                     AND o_d_id = %d 
                                     AND o_w_id = %d]])
                                  :format(table_num, no_o_id, d_id, w_id))
    else
	    o_c_id = con:query_row(([[db.orders%d.find({o_id:%d,o_d_id:%d,o_w_id:%d},{o_c_id:1})]])
                                  :format(table_num, no_o_id, d_id, w_id))
	end
--	 UPDATE orders SET o_carrier_id = :o_carrier_id
--		                WHERE o_id = :no_o_id AND o_d_id = :d_id AND
--				o_w_id = :w_id;*/
    if (drv:name() ~= "mongodb") 
    then
        con:query(([[UPDATE orders%d 
                        SET o_carrier_id = %d
                      WHERE o_id = %d 
                        AND o_d_id = %d 
                        AND o_w_id = %d]])
                      :format(table_num, o_carrier_id, no_o_id, d_id, w_id))
    else
	    con:query(([[db.orders%d.updateMany({$set:{o_carrier_id:%d},
		{o_id:%d,o_d_id:%d,o_w_id:%d}})]])
                      :format(table_num, o_carrier_id, no_o_id, d_id, w_id))
	end
--   UPDATE order_line
--		                SET ol_delivery_d = :datetime
--		                WHERE ol_o_id = :no_o_id AND ol_d_id = :d_id AND
--				ol_w_id = :w_id;*/
    if (drv:name() ~= "mongodb") 
    then
        con:query(([[UPDATE order_line%d 
                        SET ol_delivery_d = NOW()
                      WHERE ol_o_id = %d 
                        AND ol_d_id = %d 
                        AND ol_w_id = %d]])
                      :format(table_num, no_o_id, d_id, w_id))
    else
	  con:query(([[db.order_line%d.updateMany({$set:{ol_delivery_d:NOW()}},
					{ol_o_id:%d,ol_d_id:%d,ol_w_id:%d})]])
                      :format(table_num, no_o_id, d_id, w_id))
	end
--	 SELECT SUM(ol_amount) INTO :ol_total
--		                FROM order_line
--		                WHERE ol_o_id = :no_o_id AND ol_d_id = :d_id
--				AND ol_w_id = :w_id;*/

        local sm_ol_amount
    if (drv:name() ~= "mongodb") 
    then
        sm_ol_amount = con:query_row(([[SELECT SUM(ol_amount) sm
                                          FROM order_line%d 
                                         WHERE ol_o_id = %d 
                                           AND ol_d_id = %d 
                                           AND ol_w_id = %d]])
                                      :format(table_num, no_o_id, d_id, w_id))
	else 
	 sm_ol_amount = con:query_row(([[db.order_line%d.aggregate({ $match: {   $and: [    
	 { ol_o_id: %d },     
	 { ol_d_id: %d },
	 {ol_w_id:%d}
    ]
	} },
	{ $group: { sum : { $sum: "ol_amount" } } } )]]):format(table_num, no_o_id, d_id, w_id))
	end
--	UPDATE customer SET c_balance = c_balance + :ol_total ,
--		                             c_delivery_cnt = c_delivery_cnt + 1
--		                WHERE c_id = :c_id AND c_d_id = :d_id AND
--				c_w_id = :w_id;*/
--        print(string.format("update customer table %d, cid %d, did %d, wid %d balance %f",table_num, o_c_id, d_id, w_id, sm_ol_amount))  				
    if (drv:name() ~= "mongodb") 
    then
	    con:query(([[UPDATE customer%d 
                        SET c_balance = c_balance + %f,
                            c_delivery_cnt = c_delivery_cnt + 1
                      WHERE c_id = %d 
                        AND c_d_id = %d 
                        AND c_w_id = %d]])
                      :format(table_num, sm_ol_amount, o_c_id, d_id, w_id))
     else 
	 con:query(([[db.customer%d.updateMany({$set:{c_balance:$c_balance+%f,c_delivery_cnt:$c_delivery_cnt+1}},
	 {c_id:%d,c_d_id:%d,c_w_id:%d})]])
                      :format(table_num, sm_ol_amount, o_c_id, d_id, w_id))
	 end
		
	end
        
    end
    con:query("COMMIT")

end

function stocklevel()
    local table_num = sysbench.rand.uniform(1, sysbench.opt.tables)
    local w_id = sysbench.rand.uniform(1, sysbench.opt.scale)
    local d_id = sysbench.rand.uniform(1, DIST_PER_WARE)
    local level = sysbench.rand.uniform(10, 20)

    con:query("BEGIN")

--	/*EXEC_SQL SELECT d_next_o_id
--	                FROM district
--	                WHERE d_id = :d_id
--			AND d_w_id = :w_id;*/

--  What variant of queries to use for stock_level transaction
--  case1 - specification
--  case2 - modified/simplified

    local stock_level_queries="case1" 
    local d_next_o_id
    
	if (drv:name() ~= "mongodb") 
    then
    d_next_o_id = con:query_row(([[SELECT d_next_o_id 
                                     FROM district%d
             	                    WHERE d_id = %d AND d_w_id= %d]])
		                  :format( table_num, d_id, w_id))
    else
	 d_next_o_id = con:query_row(([[db.district%d.find({d_id:%d,w_id:%d},{d_next_o_id:1})]])
		                  :format( table_num, d_id, w_id))
	end
    if stock_level_queries == "case1" then 

--[[
     SELECT COUNT(DISTINCT (s_i_id)) INTO :stock_count
     FROM order_line, stock
     WHERE ol_w_id=:w_id AND ol_d_id=:d_id AND ol_o_id<:o_id AND  ol_o_id>=:o_id-20 AND s_w_id=:w_id AND s_i_id=ol_i_id AND s_quantity < :threshold;
--]]
	if (drv:name() ~= "mongodb") 
    then
    rs = con:query(([[SELECT COUNT(DISTINCT (s_i_id))
                        FROM order_line%d, stock%d
                       WHERE ol_w_id = %d 
                         AND ol_d_id = %d
                         AND ol_o_id < %d 
                         AND ol_o_id >= %d
                         AND s_w_id= %d
                         AND s_i_id=ol_i_id 
                         AND s_quantity < %d ]])
		:format(table_num, table_num, w_id, d_id, d_next_o_id, d_next_o_id - 20, w_id, level ))
    else 
	--FIXME 两个表，咋实现？
	end

--	                SELECT DISTINCT ol_i_id
--	                FROM order_line
--	                WHERE ol_w_id = :w_id
--			AND ol_d_id = :d_id
--			AND ol_o_id < :d_next_o_id
--			AND ol_o_id >= (:d_next_o_id - 20);


    else
	if (drv:name() ~= "mongodb") 
    then
    rs = con:query(([[SELECT DISTINCT ol_i_id FROM order_line%d
               WHERE ol_w_id = %d AND ol_d_id = %d
                 AND ol_o_id < %d AND ol_o_id >= %d]])
                :format(table_num, w_id, d_id, d_next_o_id, d_next_o_id - 20 ))
	else
	rs = con:query(([[db.order_line%d.aggregate({$match:{$and:{ol_w_id:%d,ol_d_id:%d,ol_o_id:{$lt:%d},ol_o_id:{$gte:%d}}}},
				[ { $group : { _id : "ol_i_id" } } ] )]])
                :format(table_num, w_id, d_id, d_next_o_id, d_next_o_id - 20 ))
	end
    local ol_i_id = {}

    for i = 1, rs.nrows do
        ol_i_id[i] = unpack(rs:fetch_row(), 1, rs.nfields)
    end

    for i = 1, #ol_i_id do

--       SELECT count(*) INTO :i_count
--                      FROM stock
--                      WHERE s_w_id = :w_id
--                      AND s_i_id = :ol_i_id
--                      AND s_quantity < :level;*/
	if (drv:name() ~= "mongodb") 
    then
        rs = con:query(([[SELECT count(*) FROM stock%d
                   WHERE s_w_id = %d AND s_i_id = %d
                   AND s_quantity < %d]])
                :format(table_num, w_id, ol_i_id[i], level ) )
    else 
		rs = con:query(([[db.stock%d.find({s_w_id:%d,s_i_id:%d,s_quantity:{$lt:%d}}).count()]])
                :format(table_num, w_id, ol_i_id[i], level ) )
	end
		local cnt
        for i = 1, rs.nrows do
            cnt = unpack(rs:fetch_row(), 1, rs.nfields)
        end

    end
    end

    con:query("COMMIT")

end

-- function purge to remove all orders, this is useful if we want to limit data directory in size

function purge()
    for i = 1, 10 do
    local table_num = sysbench.rand.uniform(1, sysbench.opt.tables)
    local w_id = sysbench.rand.uniform(1, sysbench.opt.scale)
    local d_id = sysbench.rand.uniform(1, DIST_PER_WARE)

    con:query("BEGIN")

        local m_o_id
        
        rs = con:query(([[SELECT min(no_o_id) mo
                                     FROM new_orders%d 
                                    WHERE no_w_id = %d AND no_d_id = %d]])
                                   :format(table_num, w_id, d_id))

        if (rs.nrows > 0) then
          m_o_id=unpack(rs:fetch_row(), 1, rs.nfields)
        end

        if (m_o_id ~= nil ) then 
-- select o_id,o.o_d_id from orders2 o, (select o_c_id,o_w_id,o_d_id,count(distinct o_id) from orders2 where o_w_id=1  and o_id > 2100 and o_id < 11153 group by o_c_id,o_d_id,o_w_id having count( distinct o_id) > 1 limit 1) t where t.o_w_id=o.o_w_id and t.o_d_id=o.o_d_id and t.o_c_id=o.o_c_id limit 1;
	-- find an order to delete
        rs = con:query(([[SELECT o_id FROM orders%d o, (SELECT o_c_id,o_w_id,o_d_id,count(distinct o_id) FROM orders%d WHERE o_w_id=%d AND o_d_id=%d AND o_id > 2100 AND o_id < %d GROUP BY o_c_id,o_d_id,o_w_id having count( distinct o_id) > 1 limit 1) t WHERE t.o_w_id=o.o_w_id and t.o_d_id=o.o_d_id and t.o_c_id=o.o_c_id limit 1 ]])
                                   :format(table_num, table_num, w_id, d_id, m_o_id))
	
        local del_o_id
        if (rs.nrows > 0) then
          del_o_id=unpack(rs:fetch_row(), 1, rs.nfields)
        end

        if (del_o_id ~= nil ) then 
        
        con:query(([[DELETE FROM order_line%d where ol_w_id=%d AND ol_d_id=%d AND ol_o_id=%d]])
                            :format(table_num, w_id, d_id, del_o_id))
        con:query(([[DELETE FROM orders%d where o_w_id=%d AND o_d_id=%d and o_id=%d]])
                            :format(table_num, w_id, d_id, del_o_id))
        con:query(([[DELETE FROM history%d where h_w_id=%d AND h_d_id=%d LIMIT 10]])
                            :format(table_num, w_id, d_id ))

	end

        end
        
    con:query("COMMIT")
    end
end

-- vim:ts=4 ss=4 sw=4 expandtab
