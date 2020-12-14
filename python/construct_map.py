#!/usr/bin/env python
# coding: utf-8


# 数据库连接
import psycopg2
import copy
import random
conn = psycopg2.connect(database="mississippi", user="postgres", password="pgsql", host="127.0.0.1", port="5432")
print("Connect database successfully") 
cur=conn.cursor()

# 根据关键词选出作为路径的way，新建一张表保存
cur.execute('drop table if exists qualified_way_nodes')
sql="""
create table qualified_way_nodes as select *
    from way_nodes
 	where way_id in (
		select distinct way_id
 		from way_tags
 		where k='highway' )
"""
cur.execute(sql)
conn.commit()
cur.execute("select count(distinct way_id) as count from qualified_way_nodes")
res=cur.fetchall()[0][0]
print('extract qualified ways done. there are total %d ways'%(res))
# k='cycleway' or k='oneway' or k='highway'or k='bridge' or k='bike' or k='busway' or k='expressway'or k like '%bus%' or k like 'car%' or k='horse' or k='railway' or k='footway' or k='sidewalk' or k='driveway' or k='way' or k='runway' or k='bicycle'

# 计算路径是否可以组成一个连通图
check_connectivity=False
if check_connectivity:
    node_id_list=[]
    cur.execute("select node_id from qualified_way_nodes limit 1000")
    res=cur.fetchall()
    for item in res:
        node_id_list.append(item[0])
    seed_node_id=random.choice(node_id_list)

    way_id_set=set()
    node_id_set=set()
    node_id_set.add(seed_node_id)
    while 1:
        print('number of new node_id to be query:',len(node_id_set))
        sql_str="("
        for node in node_id_set:
            sql_str+=str(node)
            sql_str+=','
        sql_str=sql_str[:-1]
        sql_str+=")"
        node_id_set=set()
        if sql_str==")":
            break
        cur.execute("select distinct way_id from qualified_way_nodes where node_id in"+sql_str)
        res=cur.fetchall()
        new_way_id_set=set()
        for item in res:
            new_way_id_set.add(item[0])
        if new_way_id_set-way_id_set==False:
            break
        candidate_way_id_set=new_way_id_set-way_id_set
        way_id_set=way_id_set|new_way_id_set
        print('the size of way_id_set is:%d'%(len(way_id_set)))
        
        new_node_id_set=set()
        print('number of new way_id to be query:',len(candidate_way_id_set))
        if len(candidate_way_id_set)==0:
            break
        sql_str="("
        for way_id in candidate_way_id_set:
            sql_str+=str(way_id)
            sql_str+=','
        sql_str=sql_str[:-1]
        sql_str+=")"
        cur.execute("select distinct node_id from qualified_way_nodes where way_id in"+sql_str)
        result=cur.fetchall()
        for item in result:
            new_node_id_set.add(item[0])
        node_id_set=new_node_id_set
    print(len(way_id_set))

    all_way_id=set()
    cur.execute("select way_id from qualified_way_nodes")
    res=cur.fetchall()
    for item in res:
        all_way_id.add(item[0])
    disqualified_way_id=all_way_id-way_id_set
    len(disqualified_way_id)

    # 删除那些不连通的way_id
    sql_delete="delete from qualified_way_nodes where way_id in ("
    for wayid in disqualified_way_id:
        sql_delete+=str(wayid)
        sql_delete+=","
    sql_delete=sql_delete[:-1]
    sql_delete+=")"
    cur.execute(sql_delete)
    conn.commit()
    cur.execute("select count(distinct way_id) as count from qualified_way_nodes")
    res=cur.fetchall()[0][0]
    print('filtered qualified ways done. there are total %d ways'%(res))


# 选出作为结点的node，新建一张表来保存

cur.execute('drop table if exists qualified_nodes')
sql="""
create table qualified_nodes as select id,geom 
from nodes 
where nodes.id in (
	select node_id
 	from qualified_way_nodes )
"""
cur.execute(sql)
conn.commit()
cur.execute("select count(id) as count from qualified_nodes")
res=cur.fetchall()[0][0]
print('extact qualified nodes node. there are total %d nodes.'%(res))


# 选出作为POI的node,新建一张表来保存

cur.execute('drop table if exists qualified_poi')
cur.execute('create table qualified_poi (node_id bigint not null,k text,category_id int)')
conn.commit()
category_sql_list=["k like 'payment%'","k like 'fuel%'","k like 'lunch%'","k like 'buffet%'","k = 'studio'","k like 'service%'",
                  "k ='vending'","k like 'school%'","k='beer'","k='restaurant' ","k like 'drink%' ","k like 'toilet%'","k like 'marketplace%'",
                   " k like 'cuisine%'","k ='brewery'","k='consulate'","k='sauna'","k='sculptor'","k='distillery'","k='furniture'",
                  "k='pastry'","k like 'theatre%'","k='university'","k='tower'","k='military'","k='store'","k='clothes'","k='lottery'",
                  "k='breakfast'","k='office' ","k='grave'"," k='parking'","k='police'","k='dinner'","k='golf'","k='preschool'","k='church'",
                  "k='coffee'"," k='atm'","k='shop'","k='massage'","k='museum'","k='bakery'","k='park'","k='hospital'","k='jewelry'",
                  "k='goods'","k='farm'","k='artwork' ","k='amenity'","k='healthcare' ","k='karaoke'","k='cafe'"]
select_poi_sql="""
insert into qualified_poi
select node_id,k ,0
from node_tags 
where %s
"""
set_category_id_sql = """
update qualified_poi set category_id=%d where %s
"""
category_count=0
for i in range(len(category_sql_list)):
    try:
        category_string=category_sql_list[i]
        cur.execute(select_poi_sql%(category_sql_list[i]))
        cur.execute(set_category_id_sql%(i,category_sql_list[i]))
        conn.commit()
        print('category %d done.'%(i))
    except Exception as e:
        print(e)
        conn.rollback()
print('extract qualified poi done.')


# 记录每个node在记录中的出现频率，将用来确定是否作为路径交点

cur.execute('drop table if exists node_freq')
#cur.execute('create table node_freq(node_id bigint not null primary key,freq bigint)')
conn.commit()
sql="create table node_freq as select node_id, count(way_id) as freq from qualified_way_nodes group by node_id"
cur.execute(sql)
conn.commit()
print('count node frequency done.')


# 构建路网数据集主要代码

# 方法定义
from math import radians, cos, sin, asin, sqrt
import time
import random
 
def haversine(lon1, lat1, lon2, lat2): # 经度1，纬度1，经度2，纬度2 （十进制度数）
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # 将十进制度数转化为弧度
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
 
    # haversine公式
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # 地球平均半径，单位为公里
    return c * r * 1000

def calculate_road_length(node_list):
    node_dist_dict=dict()
    sql_str="("
    for n in node_list:
        sql_str+=str(n)
        sql_str+=','
    sql_str=sql_str[:-1]
    sql_str+=")"
    try:
        cur.execute("select id, st_x(geom) as lon, st_y(geom) as lat from qualified_nodes where id in "+sql_str)
        result=cur.fetchall()
        for item in result:
            node_dist_dict[item[0]]=(item[1],item[2])

        length=0.0
        nodeA=[node_list[0],0.0,0.0]
        #cur.execute("select id, st_x(geom) as lon, st_y(geom) as lat from qualified_nodes where id=%d"%(int(nodeA[0])))
        #nodeAres=cur.fetchall()[0]
        nodeA[1]=node_dist_dict[nodeA[0]][0]
        nodeA[2]=node_dist_dict[nodeA[0]][1]
        node_list_len=len(node_list)
        for i in range(1,node_list_len):
            nodeB=[node_list[i],0.0,0.0]
            #cur.execute("select id, st_x(geom) as lon,st_y(geom) as lat from qualified_nodes where id=%d"%(int(nodeB[0])))
            #nodeBres=cur.fetchall()[0]
            nodeB[1]=node_dist_dict[nodeB[0]][0]
            nodeB[2]=node_dist_dict[nodeB[0]][1]
            #print(nodeA[0],nodeA[1],nodeA[2],nodeB[0],nodeB[1],nodeB[2])
            distance_res=haversine(nodeA[1],nodeA[2],nodeB[1],nodeB[2])
            #print(distance_res)
            length+=distance_res
            nodeA=copy.deepcopy(nodeB)
        return length
    except Exception as e:
        print('in calculate_road_length(): ',e)
        return 0
    

def renumber_node_id(origin_node_id,next_rank_id): 
    try:
        cur.execute("select node_new_id from node_numbers where node_id=%d"%(origin_node_id))
        result=cur.fetchall()
        if len(result)==0:
            cur.execute("insert into node_numbers values(%d,%d)"%(origin_node_id,next_rank_id))
        return cur.fetchall()[0][0]
    except:
        print('in renumber_node_id() : ',e)
        return origin_node_id
    
def write_latlon(fileObject,nodeid,nodeidNew):
    cur.execute("select id, st_x(geom) as lon, st_y(geom) as lat from qualified_nodes where id="+str(nodeid))
    result=cur.fetchall()
    longitude=result[0][1]
    latitude=result[0][2]
    fileObject.write(str(nodeidNew)+" "+str(longitude)+" "+str(latitude)+"\n")

cur.execute("select distinct way_id from qualified_way_nodes")
res_qualified_way_id=cur.fetchall()
print("there are %d records in total."%(len(res_qualified_way_id)))

road_compress=False
# 没有进行路网压缩,且没有POI
if road_compress==False:
    map_file=open('./DATA_Map_0.txt','w')
    latlon_file=open('./LATLON_Map_0.txt','w')
    #res_qualified_way_id=[[192046993,]]
    count=0
    renumbered_node=dict()
    for row in res_qualified_way_id:
        if count%100==0:
            print('building the %d th road.'%(count))
        count+=1

        #start=time.perf_counter()
        qualified_way_id=row[0]
        cur.execute("select * from qualified_way_nodes where way_id=%d order by sequence_id"%(qualified_way_id))
        res_way_node=cur.fetchall() # [[way_id,node_id,sequenced_id],[],[]]
        #end=time.perf_counter()
        #print("time for select way nodes:",end-start)

        isFirst=True
        start_node=0
        start_node_renumber=0
        start_n_lonlat=[0,0,0]
        end_node=0
        end_n_lon_lat=[0,0,0]
        end_node_renumber=0
        #print(res_way_node)
        for item in res_way_node:
            if isFirst:
                start_node=item[1]
                cur.execute("select id, st_x(geom) as lon, st_y(geom) as lat from qualified_nodes where id ="+str(start_node))
                result=cur.fetchall()
                start_n_lonlat=result[0]
                if start_node in renumbered_node:
                    # 已经被重新编码过，在字典中获取之前的编码
                    start_node_renumber=renumbered_node[start_node]
                else:
                    # 没有被重新编码，就赋予新编码
                    next_node_id=len(renumbered_node)
                    renumbered_node[start_node]=next_node_id
                    start_node_renumber=next_node_id
                latlon_file.write(str(start_node_renumber)+" "+str(start_n_lonlat[1])+" "+str(start_n_lonlat[2])+"\n")
                isFirst=False
                continue
            end_node=item[1]

            #start=time.perf_counter()
            cur.execute("select id, st_x(geom) as lon, st_y(geom) as lat from qualified_nodes where id ="+str(end_node))
            result=cur.fetchall()
            #end=time.perf_counter()
            #print("time for select lonlat:",end-start)
            end_n_lonlat=result[0]
            node_list=[start_node,end_node]
            #print(start_n_lonlat,"   ",end_n_lonlat)

            #start=time.perf_counter()
            distance=haversine(start_n_lonlat[1],start_n_lonlat[2],end_n_lonlat[1],end_n_lonlat[2])
            #end=time.perf_counter()
            #print("time for computing distance:",end-start)

            #start=time.perf_counter()
            if end_node in renumbered_node:
                # 已经被重新编码过，在字典中获取之前的编码
                end_node_renumber=renumbered_node[end_node]
            else:
                # 没有被重新编码，就赋予新编码
                next_node_id=len(renumbered_node)
                renumbered_node[end_node]=next_node_id
                end_node_renumber=next_node_id
            #end=time.perf_counter()
            #print("time for renumber:",end-start)

            map_file.write(str(start_node_renumber)+' '+str(end_node_renumber)+' '+str(distance)
                            +' '+'0'+'\n')
            latlon_file.write(str(end_node_renumber)+" "+str(end_n_lonlat[1])+" "+str(end_n_lonlat[2])+"\n")    
            start_node=end_node
            start_n_lonlat=end_n_lonlat
            start_node_renumber=end_node_renumber


    map_file.close()
    latlon_file.close()
    print("the total node number is %d"%(len(renumbered_node)))
else:            

    # 路网压缩版本

    map_file=open('./DATA_Map_0.txt','w')
    latlon_file=open('./LATLON_Map_0.txt','w')
    #res_qualified_way_id=[[192046993,]]
    count=0
    renumbered_node=dict()
    for row in res_qualified_way_id:
        if count%100==0:
            print('building the %d th road.'%(count))
        count+=1
        qualified_way_id=row[0]

        #start=time.perf_counter()
        cur.execute("select * from qualified_way_nodes where way_id=%d order by sequence_id"%(qualified_way_id))
        res_way_node=cur.fetchall() # (way_id,node_id,sequenced_id)
        #end=time.perf_counter()
        #print('select node from qualified_way_nodes time consume: %f seconds.'%(end-start))
        res_len = len(res_way_node)
        road_length=0
        numofPOI=0
        road_list=[]
        curr_road_nodes=[]
        #print(res_way_node)
        start_node=res_way_node[0][1]
        end_node=0
        curr_road_nodes.append(start_node)

        #start=time.perf_counter()
        # 对每个node_id 进行处理,拆成多条road放到road list中
        sql_str="("
        for i in res_way_node:
            sql_str+=str(i[1])
            sql_str+=','
        sql_str=sql_str[:-1]
        sql_str+=")"
        cur.execute("select * from node_freq where node_id in "+sql_str)
        result=cur.fetchall() # [node_id,freq]
        node_freq_dict=dict()
        #print(result)
        for item in result:
            node_freq_dict[item[0]]=item[1]
        is_first=True
        for node_res_item in res_way_node: # node_res_item [way_id,node_id,sequence_id]
            if is_first:
                is_first=False
                continue
            # 查询当前node的出现次数(freq)，如果freq>=2即表示它在其他way中出现过，它是另一条路的交点，应该作为路径端点
            #print(node_res_item)
            curr_node_id=node_res_item[1]
            node_freq=node_freq_dict[curr_node_id]
            #print('node frequency: ',node_freq_res)
            if node_freq>=2: # 当前node应该是路径的终点
                end_node=curr_node_id
                curr_road_nodes.append(end_node)
                road=copy.deepcopy(curr_road_nodes)
                road_list.append(road)
                start_node=end_node
                curr_road_nodes=[start_node]
            else: # 当前node只是路径上的一个转折点
                curr_road_nodes.append(curr_node_id)

        #end=time.perf_counter()
        #print('split node to multiple road time consume: %f seconds'%(end-start))
        # 对road list里面的每条road进行计算
        for r in road_list: # road_list: [[node_id1,node_id2....],[],...]
            #start=time.perf_counter()
            distance_road=calculate_road_length(r)
            #end=time.perf_counter()
            #time_elapse=end-start
            #print('distance calculation consume time: %f seconds'%(time_elapse))
            poiNum=0
            poi_nodes=[] # [[node_id category distance_from_start],....]
            node_from_start_node_list=[]
            # 寻找POI
            sql_str="("
            for n in r:
                sql_str+=str(n)
                sql_str+=','
            sql_str=sql_str[:-1]
            sql_str+=")"
            cur.execute("select * from qualified_poi where node_id in " + sql_str)
            result=cur.fetchall() #[node_id,k,category_id]
            #print(result)
            poi_node_dict=dict()
            if len(result)!=0:
                for item in result:
                    poi_node_dict[item[0]]=item[2]
            for n in r: # node id in road 
                node_from_start_node_list.append(n)

                if n in poi_node_dict.keys():
                    poiNum+=1
                    # search for poi category from database
                    #cur.execute('select category_id from qualified_poi where node_id=%d'%(n))
                    poi_category=poi_node_dict[n]
                    # calculate distance from road start node
                    distance_from_start=calculate_road_length(node_from_start_node_list)
                    poi_nodes.append([n,poi_category,distance_from_start])

            #print(r[0],r[-1],distance_road,poiNum)
            # 将node的ID进行重新编码，从0开始
            # 首先查询该node是否已经重新编码
            start_node=int(r[0])
            end_node=int(r[-1])

            if start_node in renumbered_node:
                # 已经被重新编码过，在字典中获取之前的编码
                start_node=renumbered_node[start_node]
            else:
                # 没有被重新编码，就赋予新编码
                next_node_id=len(renumbered_node)
                renumbered_node[start_node]=next_node_id
                start_node=next_node_id

            if end_node in renumbered_node:
                # 已经被重新编码过，在字典中获取之前的编码
                end_node=renumbered_node[end_node]
            else:
                # 没有被重新编码，就赋予新编码
                next_node_id=len(renumbered_node)
                renumbered_node[end_node]=next_node_id
                end_node=next_node_id

            map_file.write(str(start_node)+' '+str(end_node)+' '+str(distance_road)
                            +' '+str(poiNum)+'\n')

            if poiNum>0: # that this road contains poi
                for poi in poi_nodes:
                    # poi =[node_id,poi_category,distance_from_start,rating]
                    rating=random.randint(20,100)
                    map_file.write(str(poi[1])+' '+str(poi[2])+' '+str(rating))
                map_file.write('\n')

    map_file.close()
    for old_node_id in renumbered_node:
        new_node_id=renumbered_node[old_node_id]
        write_latlon(latlon_file,old_node_id,new_node_id)
    latlon_file.close()
    print("the total node number is %d"%(len(renumbered_node)))

