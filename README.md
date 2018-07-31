# ES_HbaseDemo

Could not locate executable null\bin\winutils.exe in the Hadoop binaries.

1.  问题导读：

     Failed to locate the winutils binary in the hadoop binary path

     java.io.IOException: Could not locate executablenull\bin\winutils.exe in the Hadoop binaries.

2.  问题解决：

      仔细查看报错是缺少winutils.exe程序。

      Hadoop都是运行在Linux系统下的，在windows下eclipse中运行mapreduce程序，要首先安装Windows下运行的支持插件（我的是hadoop2.8-common-bin.zip），

      到网上下载与自己版本吻合的插件安装并配置。

3.  安装并配置插件
 1.文件解压：
只需将hadoop-2.8.4.tar.gz解压即可，我解压到D:\hadoop-2.8.4。
 2.将解压后的文件全部放到Windows下解压的hadoop的bin目录下。

       （即eclipse中hadoop installation directory指定目录的bin目录下）如下：
       
       3.设置环境变量：
       4.至此重启电脑后，问题便可以解决了
