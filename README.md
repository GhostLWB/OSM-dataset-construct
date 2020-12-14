# OSM-dataset-construct
这是一个将OpenStreetMap格式的真实路网数据（.osm）解析并提取为Map格式的路网数据集的程序。
## 先验知识
* 何为OpenStreetMap？
>[OpenStreetMap](http://openstreetmap.org)是一个世界地图数据库，可以根据开放许可协议自由使用，通常可用于提取真实路网便于研究。OpenStreetMap的路网数据下载地址为：[OpenStreetMap data extracts](https://download.geofabrik.de/)，科学上网可使下载速度增加。
* 何为Map格式的路网数据集？
>通常图的数据结构有三种存储方式，分别是邻接矩阵，邻接表和边集数组，Map格式其实就是边集数组。在Map格式的文本文件中，每一行代表一条边或者一条边上的所有POI记录。当该行代表边时，数据格式为：`开始节点ID 结束节点ID 边的长度 边所包含的POI数量` 详见[图的存储结构之边集数组](https://blog.csdn.net/qq_38158479/article/details/104394341)以及最常用的路网数据集[Real Datasets for Spatial Databases: Road Networks and Points of Interest](http://www.cs.utah.edu/~lifeifei/SpatialDataset.htm).
* 使用本代码之前要确保什么
>本代码为python代码，因为通常路网数据集非常庞大，为了存储计算时所需节点数据，本代码需要结合postgresql数据库使用，并且使用osmosis将osm数据导入数据库中。下面是详细教程。
# 安装postgresql
postgresql和postgis要一起安装，参考博客[PostGIS的安装与初步使用](https://blog.csdn.net/qq_35732147/article/details/81169961)

# 在postgresql中创建数据库
* 使用pgAdmin 4,在database右键->create->database ,命名Mymap_DB，记得在definition选项中的template选上模板为postgis_23_sample （注意：创建数据库时，要确定模板数据库不能处于连接状态）
![pic](https://img-blog.csdn.net/20171211113814162?watermark/2/text/aHR0cDovL2Jsb2cuY3Nkbi5uZXQvdmlsaV9za3k=/font/5a6L5L2T/fontsize/400/fill/I0JBQkFCMA==/dissolve/70/gravity/SouthEast)

# 使用osmosis导入osm数据
* 下载openStreetMap地图源文件，下载地址：https://download.geofabrik.de/  ，在具体地图的页面选择Other Formats and Auxiliary Files，然后选择第一条：xxx-latest.osm.bz2,因为我们需要XML格式的数据，挂梯子下载速度更快
![](https://jiantuku-liwenbin.oss-cn-shanghai.aliyuncs.com/osmMapConstruct/dfds.jpg)
* 下载解压osmosis(下载地址：https://wiki.openstreetmap.org/wiki/Osmosis#Downloading  ），解压至任意文件夹即可
![](https://jiantuku-liwenbin.oss-cn-shanghai.aliyuncs.com/osmMapConstruct/osmosisDownload.jpg)
* 使用osmosis的脚本为pgsql数据库创建模板schema。首先找到osmosis的解压文件夹找到其script文件夹下的pgsimple_schema_0.6.sql脚本（路径为"D:\softwares\osmosis\script\pgsimple_schema_0.6.sql"），然后打开postgresql的安装目录找到bin文件夹 （D:\softwares\pgsql\bin），在这个文件夹中打开cmd，键入以下命令：
```
.\psql.exe -d Mymap_DB -U postgres -W -f "D:\softwares\osmosis\script\pgsimple_schema_0.6.sql"
```
(其中，Mymap_DB是你在postgresql中创建的数据库的名称，postgres是你的postgresql的用户名，-f后面接的是你的pgsimple_schema_0.6.sql脚本的文件路径。)
接着输入口令，也就是postgres用户的密码，看到以下界面就成功了。
![](https://jiantuku-liwenbin.oss-cn-shanghai.aliyuncs.com/osmMapConstruct/createscchema.jpg)
同时可以在pgAdmin中看到数据库中多了很多表
![](https://jiantuku-liwenbin.oss-cn-shanghai.aliyuncs.com/osmMapConstruct/dfresult.jpg)
* 使用osmosis导入路网数据。打开osmosis的解压文件夹，找到bin文件夹（D:\softwares\osmosis\bin），然后在这个bin文件夹打开cmd，命令是：
```
.\osmosis --read-xml file=[osm文件路径] --write-pgsimp database=[数据库名] user=[用户名] password=[密码] host="localhost"
```
如`.\osmosis --read-xml file="D:\paper_code\dataset\penn_map\pennsylvania-latest.osm" --write-pgsimp database="penn" user="postgres" password="1997pgsql" host="localhost"`
然后等待执行完毕。
![](https://jiantuku-liwenbin.oss-cn-shanghai.aliyuncs.com/osmMapConstruct/osmosisImport.jpg)
数据导入部分大功告成！

# 参考文献：
* 博客[PostGIS的安装与初步使用](https://blog.csdn.net/qq_35732147/article/details/81169961)
* 博客[导入OSM数据至PostgreSQL数据库](https://blog.csdn.net/vili_sky/article/details/78771276)
