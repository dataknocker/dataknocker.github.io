title: idea上运行local的spark sql hive
date: 2014-10-11 17:36:50
categories: spark sql
tags: [spark sql, hive]
---

在本机上通过idea跑spark sql进行hive查询等操作，一方面可以用于debug spark sql相关源码，另一方面可以加快开发测试进度，比如添加Udf等。
这里总共两步：1、安装hive remote metastore模式 2、idea上的相关配置开发

## hive 0.13安装 remote metastore
服务器test01作为remote端,test02作为client端, test02也需要有hadooop环境(和test01一样)
1、下载hive 0.13并解压
2、test01上配置hive-site.xml,加入mysql相关 (包括将mysql connect包放到lib中)

	<?xml version="1.0"?>  
	<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>  
	   
	<configuration>  
	  
	<property>  
	  <name>hive.metastore.warehouse.dir</name>  
	  <value>/user/hive/warehouse</value>  
	</property>  
	   
	<property>  
	  <name>javax.jdo.option.ConnectionURL</name>  
	  <value>jdbc:mysql://test01:3306/hive?createDatabaseIfNotExist=true</value>  
	</property>  
	   
	<property>  
	  <name>javax.jdo.option.ConnectionDriverName</name>  
	  <value>com.mysql.jdbc.Driver</value>  
	</property>  
	   
	<property>  
	  <name>javax.jdo.option.ConnectionUserName</name>  
	  <value>hive</value>  
	</property>  
	   
	<property>  
	  <name>javax.jdo.option.ConnectionPassword</name>  
	  <value>password</value>  
	</property>

3、启动test01上的metastore server: nohup hive - -service metastore> metastore.log 2>&1 &
   默认端口为9083 
4、配置test02上的hive-site.xml

	<?xml version="1.0"?>
	<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
	<configuration>
	    <property>
	        <name>hive.metastore.warehouse.dir</name>
	        <value>/user/hive/warehouse</value>
	    </property>

	    <property>
	        <name>hive.metastore.local</name>
	        <value>false</value>
	    </property>

	    <property>
	        <name>hive.metastore.uris</name>
	        <value>thrift://test01:9083</value>
	    </property>
	</configuration>

这样test02上通过hive就可以访问test01上的了


## idea上运行local的spark sql
前提：本机上的hive client可以使用, 同时我这里用了hadoop2.2版本
我使用的是maven 工程, pom.xml如下：

	<dependencies>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_2.10</artifactId>
            <version>1.1.0</version>

            <exclusions>
                <exclusion>
                <!--默认是1.0.4版本，要去掉-->
                    <groupId>org.apache.hadoop</groupId>
                    <artifactId>hadoop-client</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
        <!-- 引入和hadoop集群相同版本的-->
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>2.2.0</version>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-mllib_2.10</artifactId>
            <version>1.1.0</version>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_2.10</artifactId>
            <version>1.1.0</version>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-hive_2.10</artifactId>
            <version>1.1.0</version>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-source-plugin</artifactId>
                <version>2.2.1</version>
                <executions>
                    <execution>
                        <id>attach-sources</id>
                        <phase>package</phase>
                        <goals>
                            <goal>jar</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
            <plugin>
                <groupId>net.alchim31.maven</groupId>
                <artifactId>scala-maven-plugin</artifactId>
                <version>3.1.0</version>
                <executions>
                    <execution>
                        <id>compile-scala</id>
                        <phase>compile</phase>
                        <goals>
                            <goal>add-source</goal>
                            <goal>compile</goal>
                        </goals>
                    </execution>
                    <execution>
                        <id>test-compile-scala</id>
                        <phase>test-compile</phase>
                        <goals>
                            <goal>add-source</goal>
                            <goal>testCompile</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>



将hive client的hive-site.xml放到resources目录下，这样就可以访问远程的hive metadata了。

Test程序：

	object Test{
	  def main(args:Array[String]): Unit = {
	    val sc = new SparkContext("local", "test")
	    val hiveContext = new HiveContext(sc)
	    val rdd = hiveContext.sql("select * from test")
	    rdd.collect().foreach(println)
	  }
	}

运行时有可能报：

	Exception in thread "main" org.apache.hadoop.ipc.RemoteException: Server IPC version 9 cannot communicate with client version 4
这是因为org.apache.spark默认使用的是hadoop-client-1.0.4, 而我的hadoop是2.2.0, 所以版本不一致，解决方法是将hadoop-client-1.0.4改成hadoop-2.2.0， 解决方法已经在上面的pom.xml中加入了。

   


