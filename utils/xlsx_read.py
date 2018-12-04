import xlrd
import re
import time
import  datetime
import core.config.global_var as fd
from xlrd import xldate_as_tuple

__conn = fd.conn




def read_xlsx_and_insert_mysql():
    cur_time = int(time.time())
    cur_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(cur_time))
    workbook = xlrd.open_workbook('E:\\xlsx\\company.xlsx')
    booksheet = workbook.sheet_by_name('company1')
    p = list()
    #按照行列式拆分拼装数据
    for row in range(booksheet.nrows):
        row_data = []
        i = 0
        for col in range(booksheet.ncols):
            cel = booksheet.cell(row, col)
            val = cel.value
            try:
                if cel.ctype == 3:
                    date = xldate_as_tuple(val, 0)
                    val = datetime.datetime(*date).strftime("%Y-%m-%d")
                else:
                    val = str(val).replace(' ', '').replace('—', '').replace('\u3000', '').replace('.0', '').replace('省','').replace('市','').replace('&middot', '')
            except:
                pass

            if val == "00万":
                val = "0.00"

            # 多个手机号码处理
            if i == 4:
                if len(re.findall('/\d+', val)) > 0:
                    val = val[0:val.index('/')]
                if len(re.findall(';\d+', val)) > 0:
                    val = val[0:val.index(';')]
                if len(re.findall('；\d+', val)) > 0:
                    val = val[0:val.index('；')]
                if len(re.findall('、\d+', val)) > 0:
                    val = val[0:val.index('、')]
                val.replace('-','')

            if val != None and val != '' and val[-1] == ";":
                val = val.replace(val[-1], '')
            if val != None and val != '' and val[-1] == ".":
                val = val.replace(val[-1], '')

            if "人民币" in val and "万" in val :
                val = val.replace("人民币", '').replace("万", '')
            if "美元" in val and "万" in val :
                val = val.replace("美元", '').replace("万", '')
            if "-" in val and "人" in val :
                val = val.replace("人", '')
            if "平方米" in val  :
                val = val.replace("平方米", '')

            #多个邮箱处理
            if len(re.findall('com.*', val)) > 0:
                val = val[0:val.index('com')] + 'com'


            row_data.append(val)

            i += 1
            if i == 2:
                with __conn as db:
                    db.cursor.execute("select code from t_base_area where name=%s or sname=%s", (val, val))
                    code = db.cursor.fetchone()
                    if code != None:
                        row_data.append(code["code"].decode("utf-8"))
                    else:
                        row_data.append("")
            if i == 3:
                with __conn as db:
                    db.cursor.execute("select code from t_base_area where name=%s or sname=%s and parent_code = %s",(val, val, row_data[2]))
                    code = db.cursor.fetchone()
                    if code != None:
                        row_data.append(code["code"].decode("utf-8"))
                    else:
                        row_data.append("")
        print(row_data)
        with __conn as db:
            db.cursor.execute("select company_name from t_company where company_name = %s LIMIT 1",(row_data[0]))
            cname = db.cursor.fetchone()
            if cname == None:
                sql = "insert into t_company (company_name, province,province_code,city, city_code,\
                      address, linker, linker_mobile, linker_email, company_fax, company_website,\
                      company_desc, company_major, company_created_time_str, company_register_mony,\
                      company_employee_number, company_business_volume, company_workshop_area\
                      )   VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                db.cursor.execute(sql, (row_data[0],row_data[1],row_data[2],row_data[3],row_data[4],row_data[10],row_data[5],row_data[6], \
                                        row_data[7],row_data[8],row_data[9],row_data[11],row_data[12],row_data[13],row_data[14],row_data[15], \
                                        row_data[16],row_data[17]))
                db.conn.commit()

        #p.append(row_data)
    return p



if __name__ == '__main__':
    data_list = list()
    data_list = read_xlsx_and_insert_mysql()