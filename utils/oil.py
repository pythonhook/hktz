#-*- coding: UTF-8 -*-

import core.config.global_var as fd

from utils.oil_data_process import OilProcess

op = OilProcess(fd.conn)

"""
SQL:
update  `t_oil_yield_stats_ext` as a set a.country_id = (select id from `t_oil_country` where country_name = a.country_name),
a.breed_id = (select id from `t_oil_attach_breed` where breed_name = a.breed_name);
update  `t_oil_stock_stats` as a set a.oil_area_id = (select id from `t_oil_stock_area` where area_name = a.oil_area_name)
"""


#process year, month of table t_oil_sub_sales_stats
op.oil_sub_sales_stats_process()

# process year, month of table t_oil_sub_yield_stats
#op.oil_sub_yield_stats_process()

# process year, month, week, day of table t_oil_stock_stats

# process year, month of table t_oil_import_stats
#op.oil_stock_stats_process()
#op.oil_import_stats_process()

# process year, month of table t_oil_export_stats
#op.oil_export_stats_process()