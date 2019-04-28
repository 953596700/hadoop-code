
        val path:String="hdfs://master:9000/DataSet";
        var lines=sc.textFile(path);
        var links=lines.map(s=>{var split=s.split("\\t");(split(0),split(1).split(","))
            }
        )
        var ranks=links.mapValues(m=>1.0)
        for(i <- 1 to 10){
          ranks=links.join(ranks).values.flatMap({
            case (urls,rank)=>{
              var len=urls.size
              urls.map(url=>(url,rank/len))
            }
          }).reduceByKey(_+_).mapValues(p=>p*0.85+0.15)
        }

      var result=ranks.sortBy(_._2,false).map(p=>(p._1,p._2.formatted("%.10f")))
        result.saveAsTextFile("output3");
    


