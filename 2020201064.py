import sys
import csv
import re
import sqlparse
import os
from itertools import product
import itertools
from prettytable import PrettyTable

mdict = {}
column_dict = {}
x = PrettyTable()
#####################################################################################################
def getList(mdict): 
    abc = list() 
    for key in mdict.keys(): 
        abc.append(key) 
    return abc

def get_result_list(size,table_name):
	global result
	result =list
	# print(size,table_name)
	table_name=table_name+".csv"
	with open(table_name,newline='') as f:
		reader = csv.reader(f)
		result = list(reader)	
	for i in range(len(result)):
		for j in range(len(result[0])):
			result[i][j]=int(result[i][j])	

def column_index(table_names):
	# print(table_names)
	# print(mdict)
	i=0
	for table in table_names:
		if table in mdict:
			for x in mdict[table]:
				column_dict[x]=i
				i=i+1
	print(column_dict)
def aGgredateFunction(column_name,function_name,table_name):
	# print(column_name,function_name)
	# print(table_name)
	mAx=-sys.maxsize-1
	mIn=sys.maxsize
	# print(mIn)
	avg=0
	count=0
	sUm=0
	column_index=column_dict[column_name]
	if function_name == "max":
		for i in range(len(table_name)):
			if mAx<table_name[i][column_index]:
				mAx=table_name[i][column_index]
		return mAx
	if function_name == "min":
		# print("navneet")
		for i in range(len(table_name)):
			if mIn>table_name[i][column_index]:
				mIn=table_name[i][column_index]
		return mIn
	if function_name == "sum":
		for i in range(len(table_name)):
			sUm+=table_name[i][column_index]
		return sUm
	if function_name == "count":
		return len(table_name)
	if function_name == "average":
		for i in range(len(table_name)):
			sUm+=table_name[i][column_index]
		return sUm/len(table_name)							
#################################################################################################################
def rel_opr(con_val):
	opr = ""
	i = 0
	size = len(con_val)
	# print(size)
	while i<size:
		if con_val[i] == '=' and (con_val[i+1] != '=' or con_val[i+1] != '>' or con_val[i+1] != '<' or con_val[i+1] !='!'):
			opr = "="
			i=i+1
		elif con_val[i] == '>' and 	con_val[i+1] == '=':
			opr = ">="
			i=i+1
		elif con_val[i] == '<' and con_val[i+1] == '=':
			opr = "<="
			i=i+1
		elif con_val[i] == '!' and con_val[i+1] == '=':
			opr = "!="
			i=i+1
		elif con_val[i] == '>' and con_val[i+1] != '=':
			opr = '>'
			i=i+1
		elif con_val[i] == '<' and con_val[i+1] != '=':
			opr = '<'
			i=i+1
		elif con_val[i] == '=' and (con_val[i+1] == '>' or con_val[i+1] == '<' or con_val[i+1] =='!'):
			i=i+1
			sys.exit("Invalid Syntax in where condition")
		i+=1	

	return opr		
########################################################################################################
def find_operands(con_val):
	operands = list()
	try:
		rel_oprator = rel_opr(con_val)
		# print(rel_oprator)
		operands = con_val.split(rel_oprator)
		if rel_oprator != "=":
			operands.append(rel_oprator)
		else:
			operands.append("==")
	except:
		sys.exit("Error : Syntax error in where condition")

	return operands	
############################################################################################################
def evaluate_cond(lhs,rhs,mid):
	if (mid == "==" and lhs == rhs) or \
		(mid == ">=" and lhs >= rhs) or \
		(mid =="<=" and lhs <= rhs) or \
		(mid == ">" and lhs > rhs) or \
		(mid == "<" and lhs < rhs) or \
		(mid == "!=" and lhs != rhs):
		return True
	else:
		return False
################################################################################################################		
def eval_boolExp(a,b,bool_com):
	if bool_com == "and":
		return a and b
	else:
		return a or b

##################################################################################################################
def extract_aggr_col(column_name,aggr_list,columns):
	flag = 0
	columns = columns.split(",")
	# print(columns)
	agrF = ["max","min","sum","ave","cou"]
	for col in columns:
		col = col.strip()
		# print(col)
		if col[:3] in agrF:
			aggr_list.append(col.split("(")[0])
			colum=col.split("(")[1]
			column_name.append(colum[:-1])
			flag = 1
		else:
			if flag == 1:
				sys.exit("Invalid Syntax : aggregate")
			else:
				column_name.append(col)	


################################################################################################################
def solve_order_by(new_column_name_with_ord,group_by):
	column_name=new_column_name_with_ord[0]
	order=""
	if len(new_column_name_with_ord)==2:
		order=new_column_name_with_ord[1]
	else:
		order = "asc"		
	final_result = list()
	# print(column_name,order)
	index = 0
	if column_name in column_dict:
		index = int(column_dict[column_name])
	else:
		sys.exit("Inavlid query")	
	# print(index)
	if group_by==1:
		if order == "asc":
			final_result=sorted(result,key=lambda x: x[0])
		else:
			final_result=sorted(result,key=lambda x: x[0],reverse=True)
	else:	
		if order == "asc":
			final_result=sorted(result,key=lambda x: x[index])
		else:
			final_result=sorted(result,key=lambda x: x[index],reverse=True)
		
	# print(final_result)
	result.clear()
	for i in range(len(final_result)):
		result.append(final_result[i])
	if group_by==1:	
		# print(result)
		for row in result:
			print(*row,sep = ",")
	return order

###################################################################################################################
def solve_group_by(new_column_name_grp,aggr_list,column_nameA,order_by):
	column_name=new_column_name_grp
	# print(column_name,column_dict)
	index = 0
	if column_name in column_dict:
		index=column_dict[column_name]
	else:
		sys.exit("Invalid Query")
	group_by_result = list()
	group_by_result=sorted(result,key=lambda x:x[index])
	result.clear()
	# print(group_by_result)
	# print(result)
	group_by_result1=list()
	# print(index,column_name,aggr_list,column_nameA)
	flag =0
	for i in range(len(group_by_result)-1):
		if group_by_result[i][index]==group_by_result[i+1][index]:
			group_by_result1.append(group_by_result[i])
		if group_by_result[i][index]!=group_by_result[i+1][index]:
			group_by_result1.append(group_by_result[i])
			flag=1
		if flag ==1:
			ans = list()
			ans.append(group_by_result1[0][index])
			for j in range(len(aggr_list)):
				val=aGgredateFunction(column_nameA[j+1],aggr_list[j],group_by_result1)
				ans.append(val)
			# print(ans)
			result.append(ans)
			flag = 0
			del ans
			group_by_result1.clear()
	group_by_result1.append(group_by_result[-1])
	# print(group_by_result1)
	ans = list()
	ans.append(group_by_result1[0][index])
	for j in range(len(aggr_list)):
		val=aGgredateFunction(column_nameA[j+1],aggr_list[j],group_by_result1)
		ans.append(val)
	result.append(ans)
	del ans
	group_by_result1.clear()		

	if(order_by!=1):		
		# print(result)
		for row in result:
			print(*row,sep = ",")


####################################################################################################################
def solve_where_query(condition):
	var = condition.split(" ")
	# print(var)
	opr_list = list()
	for opr in var:
		if opr == "and" or opr == "or":
			opr_list.append(opr)
	# print(opr_list)
	delimiters="or","and"
	con_val= re.split('or | and',condition)
	# print(con_val)
	flag_dict = {}
	for i in range(len(result)):
		flag_dict[i]=[]
	for a in range(len(con_val)):
		# print(con_val[a])
		operands = find_operands(con_val[a])
		# print(operands)
		operands[0] = operands[0].strip()
		operands[1] = operands[1].strip()
		# print(operands[0],operands[1],operands[2])
		lhs = operands[0]
		rhs = operands[1]
		mid = operands[2]
		# print(result)
		if lhs in column_dict:
			# print("navneet")
			if rhs in column_dict:
				lhs = column_dict[lhs]
				rhs = column_dict[rhs]
				for i in range(len(result)):
					flag_dict[i].append(evaluate_cond(int(result[i][lhs]),int(result[i][rhs]),mid))		
			else:
				lhs = column_dict[lhs]
				rhs = int(rhs)
				for i in range(len(result)):
					# print("navneet",result[i][lhs],rhs,mid)s 
					flag_dict[i].append(evaluate_cond(int(result[i][lhs]),rhs,mid))
		else:
			# print("priyanka")
			if rhs in column_dict:
				lhs = int(lhs)
				rhs = column_dict[rhs]
				for i in range(len(result)):
					flag_dict[i].append(evaluate_cond(lhs,int(result[i][rhs]),mid))	
			else:
				sys.exit("Invalid condition")
	# print(flag_dict)
	final_bool_dict={}
	for i in range(len(result)):
		final_bool_dict[i]=flag_dict[i][0]
	# print(type(final_bool_dict[0]))	
	for i in range(len(result)):
		for j in range(1,len(flag_dict[0])):
			final_bool_dict[i]=eval_boolExp(final_bool_dict[i],flag_dict[i][j],opr_list[j-1])
	# print(final_bool_dict)
	where_result =list()
	for i in range(len(final_bool_dict)):
		if final_bool_dict[i]:
			where_result.append(result[i])
	# print(result)
	result.clear()
	# print(where_result)
	for i in range(len(where_result)):
		result.append(where_result[i])
	where_result.clear()
	# print(result)					

####################################################################################################################
def cross_product(table_name1,table_name2,i):
	table_name1 = table_name1 + ".csv"
	table_name2 = table_name2 + ".csv"
	with open(table_name1,'r') as f1, open(table_name2,'r') as f2:
		reader1 = csv.reader(f1)
		reader2 = csv.reader(f2)

		with open('output'+str(i)+'.csv','w') as out:
			writer = csv.writer(out)
			writer.writerows(row1 + row2 for row1, row2 in product(reader1,reader2))
	# print(table_name1,table_name2)
	output_table = "output"+str(i)
	return output_table
#######################################################################################################################
def queried_Column(table_names):
	queryColumns = list()
	for table_name in table_names:
		if table_name in mdict:
			cols  = mdict[table_name]
			for i in cols:
				queryColumns.append(table_name+"."+i)

	#print(queryColumns)
	return queryColumns
##########################################################################################################################
def project_dist(queried_column,column_name,aggr_list,table_names,new_column_name_with_ord,order_by):
	# print(result)
	# print(column_name,table_names)
	header=list()
	if len(aggr_list)!=0:
		sys.exit("Invalid Syntax")
	if len(column_name)==1:
		if column_name[0]=="*":
			for i in range(len(queried_column)):
				header.append(queried_column[i])
		else:
			header.append(queried_column[column_dict[column_name[0]]])
	elif len(column_name)>1:
		for i in range(len(column_name)):
			# print("navneet")
			header.append(queried_column[column_dict[column_name[i]]])
	else:
		sys.exit("Inavlid query")
	# print(header)
	select_column_index=list()
	for i in range(len(header)):
		for j in range(len(queried_column)):
			if header[i]==queried_column[j]:
				select_column_index.append(j)
	final_ans=list()	
	for i in range(len(result)):
		ans=list()
		for j in range(len(select_column_index)):
			ans.append(result[i][select_column_index[j]])
		final_ans.append(ans)
		del ans
	final_ans.sort()
	final_ans2 = list(final_ans for final_ans,_ in itertools.groupby(final_ans))
	# order=""
	# if order_by==1:
	# 	if len(new_column_name_with_ord)==2:
	# 		order=new_column_name_with_ord[1]
	# 	else:
	# 		order = "asc"	
	# 	if order == "asc":
	# 		final_ans3=sorted(final_ans2,key=lambda x: x[index])
	# 	elif order == "desc":
	# 		final_ans3=sorted(final_ans2,key=lambda x: x[index],reverse=True)


	# print(header)
	final_ans2.insert(0,header)
	# print(final_ans2)
	for row in final_ans2:
			print(*row,sep = ",")


##############################################################################################################################
def project(queried_column,column_name,aggr_list,table_names):
	# result.insert(0,queried_column)
	# print(aggr_list)
	# print(column_name)
	header = list()
	if len(aggr_list)!=0:
		if len(aggr_list)!=len(column_name):
			sys.exit("Invalid Query")
	if len(column_name)==1:
		if column_name[0]=="*":
			for i in range(len(queried_column)):
				header.append(queried_column[i])
		else:
			header.append(queried_column[column_dict[column_name[0]]])
	elif len(column_name)>1:
		for i in range(len(column_name)):
			header.append(queried_column[column_dict[column_name[i]]])
	else:
		sys.exit("Inavlid query")		
	print(header)
	# print(queried_column)
	select_column_index = list()
	for i in range(len(header)):
		for j in range(len(queried_column)):
			if header[i]==queried_column[j]:
				select_column_index.append(j)
	# print(select_column_index)
	# print(result)
	final_ans=list()
	function_list=["max","min","sum","count","average"]
	if len(aggr_list)>0:
		for i in range(len(aggr_list)):
			if aggr_list[i] == "count" and column_name[i] == "*":
				print("count(*)")
				print(len(result))
			elif len(aggr_list)!=0:
				print(aggr_list[i]+"("+column_name[i]+")")
				print(aGgredateFunction(column_name[i],aggr_list[i],result))
	for i in range(len(result)):
		ans=list()
		for j in range(len(select_column_index)):
			ans.append(result[i][select_column_index[j]])
		final_ans.append(ans)
		del ans
	# print(len(final_ans))
	final_ans.insert(0,header)
	if len(aggr_list)==0:
		# print(final_ans)
		for row in final_ans:
			print(*row,sep = ",")	

########################################################################################################################
def parse_query(q):
	# print(q)
	parsed_query = sqlparse.parse(q)
	parsed_query = parsed_query[0].tokens
	# print(parsed_query)
	stmnt = sqlparse.sql.Statement(parsed_query)
	stmnt=stmnt.get_type()
	stmnt=stmnt.lower()
	# print(stmnt)
	if stmnt != 'select':
		sys.exit("Invalid Syntax")
	identifier = sqlparse.sql.IdentifierList(parsed_query)
	identifier = identifier.get_identifiers()
	# print(identifier)
	identifier_list = list()
	for i in identifier:
		identifier_list.append(str(i).lower())
	print(identifier_list)

	dist = 0
	table_names = ""
	where = 0
	columns = ""
	table_flag = 0
	condition = ""
	group_by = 0
	order_by = 0

	for x in identifier_list:
		if x == "distinct":
			dist = dist + 1
		elif  x == "from":
			table_flag = 1
			continue
		elif table_flag == 1:
			table_names = x
			table_flag = 0
			continue
		elif x.startswith('where'):
			where = 1
			condition = x[6:].strip()
	new_column_name_grp = ""
	new_column_name_with_ord = ""
	order =""		
	for i in range(len(identifier_list)):
		if identifier_list[i] == "group by":
			group_by = 1
			if identifier_list[i+1] != "order by":
				new_column_name_grp = identifier_list[i+1] 
			else:
				sys.exit("Invalid query")	
		if identifier_list[i] == "order by":
			order_by = 1
			new_column_name_with_ord = identifier_list[i+1]
	# print(group_by,new_column_name_grp)				
	new_column_name_with_ord=new_column_name_with_ord.split()
	# print(new_column_name_grp,new_column_name_with_ord)					
	# print(dist,table_names,condition)		
	if identifier_list[1]=="from":
		sys.exit("Invalid Syntax")
	if where ==1 and len(condition.strip())==0:
		sys.exit("Invalid Syntax")
	if dist>1:
		sys.exit("Invalid Syntax")
	if dist == 0 and where == 0 and group_by == 0 and order_by == 0 and  len(identifier_list)>=5:
		sys.exit("Invalid Syntax")
	if group_by == 1 and order_by == 1:
		if new_column_name_grp != new_column_name_with_ord[0]:
			sys.exit("Invalid Syntax")
	if dist ==1:
		columns = identifier_list[2]
	else:
		columns = identifier_list[1]

	table_names = table_names.split(",")
	# print(table_names)
	# print("Number of tables",len(table_names))
	global queried_column
	queried_column=queried_Column(table_names)
	# print("queried column",queried_column)
	
	size = len(table_names)
	# print(size)
	cross_product_table = table_names[0]
	if size == 1:
		cross_product_table = table_names[0]
	else:	
		for i in range(0,size-1):
			cross_product_table = cross_product(cross_product_table,table_names[i+1],i)

	# print(cross_product_table)
	get_result_list(size,cross_product_table)
	# print(type(result[0][0]))
	column_index(table_names)
	# print(result)

	column_name = list()
	aggr_list = list()
	extract_aggr_col(column_name,aggr_list,columns)
	# print("queried column name",column_name)
	# print("queried aggrigate function",aggr_list)

	columns1 = columns.split(",")
	# print(len(columns1))
	flag = 0
	if group_by == 1:
		for i in range(len(columns1)):
			if columns1[i] == new_column_name_grp:
				if len(columns1)-1 != len(aggr_list):
					sys.exit("Invalid querry")
				else:
					flag = 1
		if flag == 0:
			sys.exit("Invalid query")				

	##handeling where #####
	order = ""
	# print(condition)
	if condition != "":
		solve_where_query(condition)
		# print(result)											

	if group_by == 1:
		solve_group_by(new_column_name_grp,aggr_list,column_name,order_by)
		# print(result)

	if order_by == 1:
		order=solve_order_by(new_column_name_with_ord,group_by)
		# print(result)		
	if dist == 1 and group_by !=1:
		# print("navneet")
		project_dist(queried_column,column_name,aggr_list,table_names,new_column_name_with_ord,order_by)
	if dist !=1 and group_by!=1:
		project(queried_column,column_name,aggr_list,table_names)	


#####################################################################################################################
def metadata():
	text = open('metadata.txt','r')
	flag = 0
	for t in text:
		word = t.strip().lower()
		if word == '<begin_table>':
			flag = 1
			continue
		if flag == 1:
			table_name = word
			mdict[table_name]=[]
			flag = 0
			continue
		if word != '<end_table>':
			mdict[table_name].append(word)
	# print(mdict)		

##########################################################################################################################
def main():
	metadata()
	# print(mdict)
	global all_table_name 
	all_table_name = getList(mdict)
	# print(all_table_name)
	q = sys.argv[1]
	
	if len(sys.argv) != 2:
		sys.exit("Invalid Argument")

	if q[-1] != ";":
		sys.exit("Invalid Syntax : semicolon missing")

	q = q.lower()
	# print(q)
	parse_query(q.split(";")[0].strip())


if __name__ == "__main__":
	main()