# You sql follows

# hdfs dfs -mkdir /home/omar/exam1
# hdfs dfs -put escuelasPR.csv studentsPR.csv /home/omar/exam1/
# hive

create table schools (region string, district string, city string, identifier int, name string, level string, num_series int) row format delimited fields terminated by ',';

load data inpath '/home/omar/exam1/escuelasPR.csv' into table schools;

create table students (region string, district string, identifier int, school_name string, school_level string, sex string, sid int) row format delimited fields terminated by ',';

load data inpath '/home/omar/exam1/studentsPR.csv' into table students;

# II-1
select region, city, count(*) as count from schools group by region, city order by region, count desc;

# II-2
select school_name, count(*) as count from students group by school_name order by count desc;

# II-3
select st.* from students st inner join schools sc on st.identifier = sc.identifier where sc.city in ('Ponce', 'Cabo Rojo');

# II-4
select sc.region, sc.city, count(*) as count from students st inner join schools sc on st.identifier = sc.identifier group by sc.region, sc.city order by sc.region, count desc;
