title: MapReduce在相似度计算中的应用及优化
date: 2014-04-11 11:36:54
categories: hadoop
tags: [hadoop,mapreduce,相似度计算]
---

需求：计算用户的相似度，有用户列表U和特征列表F以及用户和特征的关系<U,F>。 根据<U1,Fn> ∩ <U2, Fm>的交集数来判断U1和U2的相似度。
解决方法：
## 一、用户维度的Join
最暴力低效的方法，因为用户量一般很大，所以join效率极低。一般不考虑。
## 二、特征维度
将用户对特征的矩阵转成特征对用户的矩阵。
### 1、转成特征对用户的矩阵：F1->U1...Un  
	map: context.write(F, U)
	reduce: context.write(F,List<U>)

### 2、计算相似度

各种解决方案如下：
#### (1)直接输出UxUy pair(IO密集型)
map:  将user list拆成各user pair对并输出，具体如下所示范例(代码只是伪码)：

    String data[] = value.toSting().split("\t");
    String users[] = data[1].split(",");//user间以,分隔
    for(int i = 0; i < users.length; i ++){
	   for(int j = i + 1; j < users.length; j ++){
            //判断users[i]与users[j]的大小 
		  context.write(users[i]+"_"+users[j], 1);//这里要加上users[i]与users[j]大小的判断，小放前，大放后，便于后面的操作
        }
    }

reduce: 对每个user pair的value list进行求和，即是这两个用户的相似度
缺点：map端输出的user pair很多(O(N*N))，使reducer的shuffle成为瓶颈
#### (2)按各user进行聚合(计算密集型)
< u1,u3,u5,u2 >，按(1)的输出是< u1_u3-->1 >,  < u1_u5-->1 >,  < u1_u2-->1>, < u3_u5-->1 >, < u3_u2-->1 >,< u5_u2-->1 >。  (1是次数)
按user聚合的结果是：< u1-->u2,u3,u5 > ,< u2-->u3,u5 >,< u3-->u5 >，输出数为N(U)-1。
该方案需要对user list进行排序，便于后面reduce进行按userid聚合，如一个user list输出的是< u1-->u2,u3,u5 >，另一个是< u1-->u3,u5 >,这样reduce时就是u1->u2,u3,u5,u3,u5。
而如果不排序的话就需要再弄一个job进行操作：如< u1-->u2,u3,u5 >, < u2-->u1,u3,u5 > 。 这样会得到< u1_u2 >与< u2_u1 >，还需要job进行一次合并求和处理。
mapper:

    StringBuilder uuidListStr = new StringBuilder();
    String data[] = value.toString().split("\t");
    String uuidArr[] = data[1].split(",");
    Arrays.sort(uuidArr);
    for(int i = 0; i < uuidArr.length; i ++){
        for(int j = i + 1; j < uuidArr.length; j ++){
            uuidListStr.append(uuidArr[j]).append(",");
        }
    	if(uuidListStr.length() > 0){
    	    uuidListStr.deleteCharAt(uuidListStr.length() - 1);
    	    context.write(new Text(uuidArr[i]), new Text(uuidListStr.toString()));
    	    uuidListStr = new StringBuilder();
    	}
    }        
reduce: 会得到u1->u2,u3,u5,u3,u5,  计算values中各用户出现的个数< ux,count >, 然后输出count即可

    //利用hashMap来管理各user的次数
    Map<String, Integer> countMap = new HashMap<String, Integer>();
    for (Text v : values) { //每个v都是u1,u2,u3这样的形式
        uuids = v.toString().split(",");
        for (int i = 0; i < uuids.length; i++) {
            uuid = uuids[i];
            tmp = countMap.get(uuid);
            if (null == tmp) {
                countMap.put(uuid, new Integer(1));
            } else {
                countMap.put(uuid, tmp + 1);
            }
        }
    }
    for (Map.Entry<String, Integer> entry : countMap.entrySet()) {
        context.write(new Text(key.toString() + "_" + entry.getKey()), new IntWritable(entry.getValue()));
    }
该方案有个瓶颈是map中自己实现的排序，可能某个F下用户数特别大，会造成数据倾斜，有的user list特别大，排序花费时间长，导致整个任务变慢(计算密集型)。一种思路是将splitSize变小，如从默认的64M变成8M，这样InputSplit数将变多，即Mapper变多，各个Mapper处理的数据量变小，充分发挥并行的优势。
具体设置splitSize的代码：

    //splitSize = max(minSize, min(maxSize, blockSize))
    conf.set("mapred.min.split.size", 8 * 1024 * 1024 + "");
    conf.set("mapred.max.split.size", 8 * 1024 * 1024 + "");
 
#### (3)排序放到Reducer端
在转置特征对用户的矩阵的job中reduce已经得到了各F的user list，则可以直接对user list进行排序并输出按user聚合的结果。
##### a、reduce方法中对user list进行排序
会遇到和(2)方案中一样的数据倾斜问题，且无法像(2)那样减少splitSize来减少各Mapper的处理数据量，增大Mapper数。 该方案一般会作死，数据量大时不用考虑。
##### b、利用Reducer的Sort功能
需要覆盖的类：
MapOutputKey：其中的compareTo用于map/reduce各阶段进行排序的依据
Partitioner: 用于partition分到哪个区
GroupingComparator：用于reduce的sort-->reduce中 key迭代时的分组。
Reducer的各阶段为：Shuffle/Merge,  Sort, Reduce, Output。其中Sort与Reduce是并行的。Sort迭代遍历得到的记录会进行grouping,从而得到reduce方法中的values。
Sort会将各文件按Key(MapOutputKey)的大小建最小堆，每取一个最小Key的记录, 都会到GroupingComparator进行判断(具体源码没研究，不过这里面的实现应该是会保存上一个记录的Key, 如果当前记录与上一Key 通过GroupingComparator方法得到的结果是一样的话，则把当前记录加到group的记录列表中，该列表元素顺序是按插入顺序的；如果不一样的话，就将Key以前列表的数据传到reduce方法，并清空group的记录列表)
我们可以创建自己的MapOutputKey

    public class GeoMapOutputKey implements Writable,WritableComparable<GeoMapOutputKey> {
        private Text geohash = new Text();//相当于Feature
        private Text uuid = new Text();  //相当于user
        public GeoMapOutputKey(){}
        public GeoMapOutputKey(String geohash, String uuid){
            this.geohash.set(geohash);
            this.uuid.set(uuid);
        }
        public Text getGeohash() {
            return geohash;
        }
        public void setGeohash(Text geohash) {
            this.geohash = geohash;
        }
        public Text getUuid() {
            return uuid;
        }
        public void setUuid(Text uuid) {
            this.uuid = uuid;
        }
        @Override
        public void write(DataOutput dataOutput) throws IOException {
            geohash.write(dataOutput);
            uuid.write(dataOutput);
        }
        @Override
        public void readFields(DataInput dataInput) throws IOException {
            geohash.readFields(dataInput);
            uuid.readFields(dataInput);
        }
        @Override
        //重点是这个compareTo方法，会先根据geohash进行排序，再根据uuid进行排序
        public int compareTo(GeoMapOutputKey o) {
            int compareValue = this.geohash.compareTo(o.geohash);
            if(compareValue == 0){
                compareValue = this.uuid.compareTo(o.uuid);
            }
            return compareValue;
        }
        @Override
        public int hashCode() {
            return geohash.hashCode() * 163 + uuid.hashCode() * 163;
        }
        @Override
        public boolean equals(Object o) {
            if (o instanceof GeoMapOutputKey) {
                GeoMapOutputKey ok = (GeoMapOutputKey) o;
                return geohash.equals(ok.getGeohash()) && uuid.equals(ok.getUuid());
            }
            return false;
        }
        @Override
        public String toString() {
            return geohash + "\t" + uuid;
        }
    }
通过这个GeoMapOutputKey，就可以保存先按Feature进行排序，再按user进行排序。(map输出有N行记录，Reducer默认情况下也要对这N行记录进行compare，所以性能没有什么影响)。
Mapper的map阶段的输出会调用Partitioner方法进行决定分区，默认情况下会按feature+user进行分区，我们需要按Feature进行分区，所以要覆盖：

    public class GeoPartitioner extends Partitioner<GeoMapOutputKey, Text> {
        @Override
        public int getPartition(GeoMapOutputKey geoMapOutputKey, Text text, int numPartitions) {
       //这里的geohash就是feature
            return Math.abs(geoMapOutputKey.getGeohash().hashCode()) % numPartitions;
        }
    }

默认情况下Reducer的GroupingComparator会按Key进行grouping聚合操作，这样reduce方法中的key就是feature_u1这样的，没多大帮助，所以我们要自定义GroupingComparator，让相同feature的聚合在一起，即reduce方法中的key是feature。

    public class GeoGroupingComparator extends WritableComparator {
        protected GeoGroupingComparator() {
            super(GeoMapOutputKey.class, true);
        }
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            GeoMapOutputKey ok1 = (GeoMapOutputKey) a;
            GeoMapOutputKey ok2 = (GeoMapOutputKey) b;
            //这里只对feature进行比较，即会按feature进行grouping聚合
            return ok1.getGeohash().compareTo(ok2.getGeohash());
        }
    }
设置这两个类的代码：

    job.setMapOutputKeyClass(GeoMapOutputKey.class);
    job.setGroupingComparatorClass(GeoGroupingComparator.class);
通过这样的设置，就可以实现Sort最小堆是按先feature再user进行排序，而聚合时又是按feature进行聚合。
reduce中的key是<F,u1>,<F,u2>,<F,u5>中的某一个(第一个?)，value是< u1,u2,u5 >
这个job的输出是U1–>u2,u3,u5这样的形式。
下一个job的mapper只要context("u1","u2,u3,u5")，而reducer和(2)中的reducer操作一样。

第(3)种方案是最优的，处理速度比前面的高效很多。