import csv
import sqlparse as sp
from sqlparse.sql import IdentifierList, Identifier, Where, Comparison
from sqlparse.tokens import Keyword, DML, Newline, Whitespace, Text, Token

'''Data loading functions'''

def extract_metadata():
    path = ""
    metafile = open('files/metadata.txt', 'r') 
    metalines = metafile.readlines()
    tables_meta = {}
    tables_list = {}
    is_rec = False
    is_tname = False
    cur_table = ""
    for line in metalines:
        if line.startswith('<begin_table>'):
            is_rec = True
            is_tname = True
        elif line.startswith('<end_table>'):
            if_rec = False
        elif is_tname:
            cur_table = str(line).strip().lower()
            tables_meta[cur_table] = []
            tables_list[cur_table] = []
            is_tname = False
        else:
            tables_meta[cur_table].append(str(line).strip().lower())
    return tables_meta,tables_list
            

def extract_csvdata_byrows(tables_list):
    tables_data = tables_list
    for tn in tables_list: 
        with open('files/'+tn+'.csv', newline='') as table_file:
            all_data = csv.reader(table_file,delimiter=',')
#             print(all_data)
            for row in all_data:
                tables_data[tn].append([int(x) for x in row])
    return tables_data


'''SQL functions'''
def remove_wspaces(parsed_sql):
    modf_parsed_sql = []
    for token in parsed_sql:
        if token.is_whitespace:
            continue
        modf_parsed_sql.append(token)
    return modf_parsed_sql


def attr_condition(cndtn):
    c_attr = {'id1':'', 'opr':'', 'id2':''}
    for token in cndtn:
        if isinstance(token,Identifier):
            if not c_attr['id1']:
                c_attr['id1'] = token.get_name()
            else:
                c_attr['id2'] = token.get_name()
        elif token.ttype is Token.Operator.Comparison:
            c_attr['opr'] += token.value
        elif token.ttype is Token.Literal.Number.Integer:
            if not c_attr['id1']:
                c_attr['id1'] = int(token.value)
            else:
                c_attr['id2'] = int(token.value)
    return c_attr
    

def process_where(where_stmnt):
    where_dict = {'andor':"", 'conditions':[]}
    modf_where_stmnt = remove_wspaces(where_stmnt)
    for token in modf_where_stmnt:
        if token.ttype is Keyword and token.value == 'where':
            continue
        if token.ttype is Keyword and token.value == 'and':
            where_dict['andor'] = 'and'
        elif token.ttype is Keyword and token.value == 'or':
            where_dict['andor'] = 'or'
        elif isinstance(token,sp.sql.Comparison):
            where_dict['conditions'].append(token)
        elif isinstance(token,sp.sql.Identifier):
            temp.append(token)
        else:
            print(invalid_msg,": Where")
            exit(0)
    return where_dict
    
def get_aggregate_fn(token):
    aggfncs_list = ['count','max','min','sum','avg']
#     print(token,type(token))
    aggfn_dict = {}
    for af in aggfncs_list:
        if af+'(' in token:
            aggfn_dict['func'] = af
            aggfn_dict['col'] = token[token.find('(')+1:token.find(')')]
    return aggfn_dict

    
def process_query(parsed_sql):
    modf_parsed_sql = remove_wspaces(parsed_sql)
    curr_token = ""
    q_columns,q_tables,q_groupby,q_conditions = [],[],[],{}
    q_aggfn = {'func':[], 'col':[]}
    q_orderby = {'col':'', 'order':None}
    q_distinct = False
    for token in modf_parsed_sql:
#         print("========",token,token.ttype,type(token))
        if token.ttype is DML and token.value == 'select':
            curr_token = 'select'
            continue
        if token.ttype is Keyword and token.value == 'from':
            if curr_token not in ['select','distinct']:
                print(invalid_msg,": From")
                exit(0)
            curr_token = 'from'
            continue
        if isinstance(token,Where):
            if curr_token not in ['from']:
                print(invalid_msg,": Where")
                exit(0)
            curr_token = 'where'
            q_conditions = process_where(token)
            continue
        if token.ttype is Keyword and token.value == 'group by':
            if curr_token not in ['from','where']:
                print(invalid_msg,": Group by")
                exit(0)
            curr_token = 'group by'
            continue
        if token.ttype is Keyword and token.value == 'order by':
            if curr_token not in ['from','where','group by']:
                print(invalid_msg,": Order by")
                exit(0)
            curr_token = 'order by'
            continue
        if curr_token == 'select':
            if token.ttype is Keyword and token.value == 'distinct':
                if q_columns or q_aggfn['func']:
                    print(invalid_msg,": Distinct")
                    exit(0)
                q_distinct = True
            elif isinstance(token, IdentifierList):
                for c in token.get_identifiers():
#                     print("+++++",str(c),c.get_name())
                    aggfn = get_aggregate_fn(str(c))
                    if aggfn:
                        if q_aggfn['func']:
                            q_aggfn['func'].append(aggfn['func'])
                            q_aggfn['col'].append(aggfn['col'])
                        else:
                            q_aggfn['func'] = [aggfn['func']]
                            q_aggfn['col'] = [aggfn['col']]
                    else:
                        q_columns.append(c.get_name())
            elif isinstance(token, Identifier):
                q_columns.append(token.get_name())
            elif token.ttype is Token.Wildcard:
                q_columns = ['*']
            elif isinstance(token,sp.sql.Function):
                aggfn = get_aggregate_fn(str(token))
                if aggfn:
                    if q_aggfn['func']:
                        q_aggfn['func'].append(aggfn['func'])
                        q_aggfn['col'].append(aggfn['col'])
                    else:
                        q_aggfn['func'] = [aggfn['func']]
                        q_aggfn['col'] = [aggfn['col']]
            else:
                print(invalid_msg,": Select")
                exit(0)
        elif curr_token == 'from':
            if isinstance(token, IdentifierList):
                for t in token.get_identifiers():
                    q_tables.append(t.get_name())
            elif isinstance(token, Identifier):
                q_tables.append(token.get_name())
            else:
                print(invalid_msg,": From")
                exit(0)
        elif curr_token == 'group by':
            if isinstance(token, IdentifierList):
                for c in token.get_identifiers():
                    q_groupby.append(c.get_name())
            elif isinstance(token, Identifier):
                q_groupby.append(token.get_name())
            elif token.ttype is Token.Wildcard:
                q_groupby = ['*']
            else:
                print(invalid_msg,": Group by")
                exit(0)
        elif curr_token == 'order by':
            if isinstance(token, Identifier):
                q_orderby['col'] = token.get_name()
                q_orderby['order'] = token.get_ordering()
            else:
                print(invalid_msg,": Order by")
                exit(0)
    q_attributes = {}
    q_attributes['q_tables'] = q_tables
    q_attributes['q_cols'] = q_columns
    q_attributes['q_conditions'] = q_conditions
    q_attributes['q_groupby'] = q_groupby
    q_attributes['q_aggfn'] = q_aggfn
    q_attributes['q_distinct'] = q_distinct
    q_attributes['q_orderby'] = q_orderby
    return q_attributes

def join_tables(tables):
    join_data = tables_data_byrows[tables[0]]
    disp_cnames = []
    disp_cnames += tables_meta[tables[0]]
    for t in tables[1:]:
        temp_join = []
        for rj in join_data:
            for rt in tables_data_byrows[t]:
                temp_join.append(rj+rt)
        join_data = temp_join
        disp_cnames += tables_meta[t]
    return join_data, disp_cnames

def get_distinct(q_rows):
    return set([tuple(row) for row in q_rows])
    
def select_rows(q_rows,q_tables,q_cols):
    cols_idx = []
    allcols = []
    for t in q_tables:
        allcols += tables_meta[t]
    if q_cols[0]=='*':
        return q_rows,allcols
    for c in q_cols:
        cols_idx.append(allcols.index(c))
    sel_rows = []
    for row in q_rows:
        tmprow = []
        for i in cols_idx:
            tmprow.append(row[i])
        sel_rows.append(tmprow)
    return sel_rows,q_cols

def display(q_rows,q_tables,disp_cnames):
#     print("--------OUTPUT--------")
    j=1
    for c in disp_cnames:
        tab = ''
        for t,cl in tables_meta.items():
            if c in cl:
                tab = t
        if j<len(disp_cnames):
            if not tab:
                print(c,end=',')
            else:
                print(tab+'.'+c,end=',')
        else:
            if not tab:
                print(c)
            else:
                print(tab+'.'+c)
        j+=1
#     print()
    for row in q_rows:
        for i in range(len(disp_cnames)):
            if i<len(disp_cnames)-1:
                print(row[i],end=',')
            else:
                print(row[i])
#         print()
#     print("\nRows displayed:",len(q_rows))
    
def compare_cols(row,c_attr,fc1,fc2,xc1,xc2):
    if (c_attr['opr'] == "=" and ((fc1 and fc2 and row[xc1]==row[xc2]) or ((not fc2) and row[xc1]==c_attr['id2']))) or        (c_attr['opr'] == ">" and ((fc1 and fc2 and row[xc1]>row[xc2]) or ((not fc2) and row[xc1]>c_attr['id2']))) or        (c_attr['opr'] == "<" and ((fc1 and fc2 and row[xc1]<row[xc2]) or ((not fc2) and row[xc1]<c_attr['id2']))) or        (c_attr['opr'] == "!=" and ((fc1 and fc2 and row[xc1]!=row[xc2]) or ((not fc2) and row[xc1]!=c_attr['id2']))) or        (c_attr['opr'] == "<=" and ((fc1 and fc2 and row[xc1]<=row[xc2]) or ((not fc2) and row[xc1]<=c_attr['id2']))) or        (c_attr['opr'] == ">=" and ((fc1 and fc2 and row[xc1]>=row[xc2]) or ((not fc2) and row[xc1]>=c_attr['id2']))):
        return True
    return False
    
def execute_where(q_rows,q_tables,q_where):
    cnames = []
    for t in q_tables:
        cnames += tables_meta[t]
    sel_rows = [False for i in range(len(q_rows))]
    ci=0
    for cndtn in q_where['conditions']:
        cndtn = remove_wspaces(cndtn)
        c_attr = attr_condition(cndtn)
#         print(c_attr)
        fc1=False
        fc2=False
        xc1=-1
        xc2=-1
        if isinstance(c_attr['id1'],str):
            if c_attr['id1'] not in cnames:
                print(invalid_msg,": Column does not exist")
                exit(0)
            fc1 = True
            xc1 = cnames.index(c_attr['id1'])
        if isinstance(c_attr['id2'],str):
            if c_attr['id2'] not in cnames:
                print(invalid_msg,": Column does not exist")
                exit(0)
            fc2 = True
            xc2 = cnames.index(c_attr['id2'])
#         print(fc1 and fc2)
        if ci==0:
            r = 0
            for row in q_rows:
                sel_rows[r] = compare_cols(row,c_attr,fc1,fc2,xc1,xc2)
                r+=1
        else:
            r=0
            if q_where['andor']=='and':
                for row in q_rows:
                    if sel_rows[r]:
                        sel_rows[r] = compare_cols(row,c_attr,fc1,fc2,xc1,xc2)
                    r+=1
            elif q_where['andor']=='or':
                for row in q_rows:
                    if not sel_rows[r]:
                        sel_rows[r] = compare_cols(row,c_attr,fc1,fc2,xc1,xc2)
                    r+=1
        ci+=1
    r=0
    new_qrows = []
    for row in q_rows:
        if sel_rows[r]:
            new_qrows.append(row)
        r+=1
    return new_qrows

def execute_aggfn(grp_rows,q_aggfn,cnames,q_grpcols='',q_cols=''):
    grp_out = []
    disp_cnames = []
    if q_grpcols:
        for gc in q_grpcols:
            if gc in q_cols:
                gcix = cnames.index(gc)
                grp_out.append(grp_rows[0][gcix])
                disp_cnames.append(gc)
    for afi in range(len(q_aggfn['func'])):
        if q_aggfn['func'][afi] == 'max':
            ci = cnames.index(q_aggfn['col'][afi])
            grp_out.append([max(i) for i in zip(*grp_rows)][ci])
            disp_cnames.append("max("+q_aggfn['col'][afi]+")")
        elif q_aggfn['func'][afi] == 'min':
            ci = cnames.index(q_aggfn['col'][afi])
            grp_out.append([min(i) for i in zip(*grp_rows)][ci])
            disp_cnames.append("min("+q_aggfn['col'][afi]+")")
        elif q_aggfn['func'][afi] == 'sum':
            ci = cnames.index(q_aggfn['col'][afi])
            grp_out.append([sum(i) for i in zip(*grp_rows)][ci])
            disp_cnames.append("sum("+q_aggfn['col'][afi]+")")
        elif q_aggfn['func'][afi] == 'count':
            grp_out.append(len(grp_rows))
            disp_cnames.append("count("+q_aggfn['col'][afi]+")")
        elif q_aggfn['func'][afi] == 'avg':
            ci = cnames.index(q_aggfn['col'][afi])
            grp_out.append(round([sum(i) for i in zip(*grp_rows)][ci]/len(grp_rows),2))
            disp_cnames.append("avg("+q_aggfn['col'][afi]+")")
#     print(grp_out)
    return [grp_out],disp_cnames
    
            
def execute_groupby(q_rows,q_grpcols,cnames,q_aggfn,q_cols):
    gcol_idx = [cnames.index(gc) for gc in q_grpcols]
#     gcol_tuples = [row[gcol_idx] for row in q_rows]
    gcol_tuples = []
    for row in q_rows:
        gcvl = []
        for gci in gcol_idx:
            gcvl.append(row[gci])
        gcol_tuples.append(tuple(gcvl))
    gcval_map = {}
    for i in range(len(gcol_tuples)):
        if gcol_tuples[i] not in gcval_map:
            gcval_map[gcol_tuples[i]] = []
        gcval_map[gcol_tuples[i]].append(i) #+=
    new_grows = []
    disp_cnames=''
    for x,y in gcval_map.items():
        if q_aggfn['func']:
            grp_rows = []
            for i in y:
                grp_rows.append(q_rows[i])
            grp_rows,disp_cnames = execute_aggfn(grp_rows,q_aggfn,cnames,q_grpcols,q_cols)
#             new_grows.append(grp_rows)
            new_grows += grp_rows
        else:
            new_grows.append(list(x))
#             disp_cnames = q_cols
    if not disp_cnames:
        disp_cnames = [cnames[i] for i in gcol_idx]
#     print(new_grows)
    return new_grows,disp_cnames

# def flatten_groups(q_grows):
#     flat_rows = []
#     for g in q_grows:
#         flat_rows += g
#     return flat_rows

def execute_orderby(q_rows,q_orderby,disp_cnames):
    ci = disp_cnames.index(q_orderby['col'])
    return(sorted(q_rows, key = lambda x: x[ci], reverse=(q_orderby['order']=='DESC')))   

def check_cols(cnames,q_attr):
    if q_attributes['q_cols']!=['*'] and any(c not in cnames for c in q_attributes['q_cols']):
        print(invalid_msg,": Column does not exist")
        exit(0)
    if q_attributes['q_orderby']['col'] and (q_attributes['q_orderby']['col'] not in cnames):
        print(invalid_msg,": Column does not exist")
        exit(0)
    if q_attr['q_groupby']:
        if any(c not in q_attr['q_groupby'] for c in q_attributes['q_cols']):
            print("Group by:",invalid_msg)
            exit(0)
        if q_attributes['q_orderby']['col'] and q_attributes['q_orderby']['col'] not in q_attr['q_groupby']:
            print(invalid_msg,": Group by and Order by should be on the same column")
            exit(0)
    if q_attributes['q_aggfn']['func'] and q_attributes['q_cols']:
        if any(c not in q_attr['q_groupby'] for c in q_attributes['q_cols']):
            print(invalid_msg,": Aggregate function & column cannot be selected together")
            exit(0)
    if q_attributes['q_aggfn']['col'] and any(c not in cnames+['*'] for c in q_attributes['q_aggfn']['col']):
        print(invalid_msg,": Column does not exist")
        exit(0)
    return True

def execute_query(q_attributes):
    q_data = []
    cnames = []
    disp_cnames = []
    flag = False
    if not q_attributes['q_tables'] or any(t not in tables_meta for t in q_attributes['q_tables']):
        print(invalid_msg,": Table does not exist")
        exit(0)
    for t in q_attributes['q_tables']:
        cnames += tables_meta[t]
    check_cols(cnames,q_attributes)
    if len(q_attributes['q_tables'])>1:
        q_data,disp_cnames = join_tables(q_attributes['q_tables'])
    else:
        q_data = tables_data_byrows[q_attributes['q_tables'][0]]
        disp_cnames = cnames
    if q_attributes['q_conditions']:
        q_data = execute_where(q_data,q_attributes['q_tables'],q_attributes['q_conditions'])
    if q_attributes['q_groupby']:
        q_data,disp_cnames = execute_groupby(q_data,q_attributes['q_groupby'],disp_cnames,q_attributes['q_aggfn'],q_attributes['q_cols'])
        flag = True
    if q_attributes['q_aggfn']['func'] and not q_attributes['q_groupby']:
        q_data,disp_cnames = execute_aggfn(q_data,q_attributes['q_aggfn'],disp_cnames)
        flag = True
    if not flag:
        q_data,disp_cnames = select_rows(q_data,q_attributes['q_tables'],q_attributes['q_cols'])
    if q_attributes['q_distinct']:
        q_data = get_distinct(q_data)
    if q_attributes['q_orderby']['col']:
        q_data = execute_orderby(q_data,q_attributes['q_orderby'],disp_cnames)
    display(q_data,q_attributes['q_tables'],disp_cnames)
    
        
if __name__ == "__main__":
    invalid_msg = 'INVALID QUERY'
    tables_meta,tables_list = extract_metadata()
    tables_data_byrows = extract_csvdata_byrows(tables_list)
#     print(tables_data_byrows)

    qry_input = input().strip().lower()
    # qry_input = "select A, max(b) from table1 where a >= 640 and b > 311;".lower()
    if qry_input[-1] != ';':
        print("Semicolon missing")
        exit(0)
    parsed_sql = sp.parse(qry_input[:len(qry_input)-1])[0]
#     print(parsed_sql.tokens)
    q_attributes = process_query(parsed_sql)
#     print(q_attributes)
    execute_query(q_attributes)

