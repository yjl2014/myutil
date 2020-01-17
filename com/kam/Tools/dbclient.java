package com.kam.Tools;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mysql.jdbc.ResultSetMetaData;

public class dbclient {
    //声明Connection对象
	private Connection con;
    //驱动程序名
	private String driver = "com.mysql.jdbc.Driver";
    //URL指向要访问的数据库名mydata
	private String url;
	//MySQL配置时的数据库
	private String dbname;
	//MySQL配置时的端口号
	private int port;
    //MySQL配置时的用户名
	private String user;
    //MySQL配置时的密码
	private String password;
    //Sql语句
	private String sql;
	//是否使用SSL连接
	private String usessl = "true";
	
	public void setDbLink(String url,int port,String dbname,String user,String password)
	{
		this.url = url;
		this.port = port;
		this.dbname = dbname;
		this.user = user;
		this.password = password;
		return ;
	}
	
	public boolean setssl(String ssl)
	{
		this.usessl = ssl;
		return true;
	}
	
	public void setSql(String sql)
	{
		this.sql = sql;
		return ;
	}
	
	public boolean dbConnect(){
        //加载驱动程序
      try {
    	  
			Class.forName(this.driver);
			//1.getConnection()方法，连接MySQL数据库！
			
			String linkurl;
			linkurl = "jdbc:mysql://" + this.url + ":" + this.port + "/" + this.dbname + "?useSSL="+this.usessl+"&useUnicode=true&characterEncoding=UTF-8";
			this.con = DriverManager.getConnection(linkurl,this.user,this.password);
			if(!this.con.isClosed())
			{
				return true;
			}else{
				System.out.println("connecting to the Database Error!");
				return false;
			}			
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
      
      return false;
	}
	
	public boolean dbClose()
	{
		try {
			if(this.con != null)
			{
				this.con.close();
				return true;
			}else{
				return false;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	public List<Map<String, Object>> getResultList(String sql)
	{
		this.sql = sql;
		return getResultList();
	}
	
	public List<Map<String, Object>> getResultList()
	{
	   List<Map<String, Object>> list = new ArrayList<Map<String,Object>>(); 
		try {
			//2.创建statement类对象，用来执行SQL语句！！
			Statement statement;
			statement = this.con.createStatement();
			//3.ResultSet类，用来存放获取的结果集！！
			ResultSet rs = statement.executeQuery(this.sql);
			ResultSetMetaData md = (ResultSetMetaData) rs.getMetaData(); //获得结果集结构信息,元数据  
			int columnCount = md.getColumnCount();   //获得列数   
			while (rs.next()) {  
			    Map<String,Object> rowData = new HashMap<String,Object>();  
			    for (int i = 1; i <= columnCount; i++) {
			    	String columnName = md.getColumnName(i);
			    	String lname = md.getColumnLabel(i);
			    	if(lname!=null){
			    		if(!lname.isEmpty()){
			    			columnName=lname;
			    		}
			    	}
			        rowData.put(columnName, rs.getObject(i));
			      }  
			    list.add(rowData);  
			}  
			rs.close();
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

        return  list;
	}
	
	public boolean executeQuerySql(String sql)
	{
		this.sql = sql;
		
		Statement statement;
		try {
			statement = this.con.createStatement();
			statement.executeQuery(this.sql);
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return true;
	}
	
	public boolean executeSql(String sql)
	{
		this.sql = sql;
		return executeSql();
	}
	
	public boolean executeSql()
	{
		try {
			Statement statement;
			statement = this.con.createStatement();
			int i = statement.executeUpdate(this.sql);
			statement.close();
			if (i >= 0) {
				return true;
			}else{
				return false;
			}	
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}
}

/********************************************
使用说明：
dbclient dbc = new dbclient();
//1.设置数据连接
dbc.setDbLink("localhost",3306,"mysql", "root", "123456");
//2.连接数据库
if(dbc.dbConnect())
{
	//3.执行增删建库表等
	dbc.executeSql("drop database jtest");
	if(dbc.executeSql("create database jtest")){
		System.out.println("jtest database create successed!");
		if(dbc.executeSql("create table jtest.jtable(username varchar(50),sex int(11))"))
		{
			System.out.println("jtable create successed!");
			if(dbc.executeSql("insert into jtest.jtable(username,sex) values('w1',1),('w2',0),('w3',1)"))
			{
				System.out.println("insert record successed!");
				//4.结果集查询
				List<Map<String, Object>> rslist = new ArrayList<Map<String,Object>>();
				rslist = dbc.getResultList("select * from jtest.jtable");
				for(int i=0; i<rslist.size(); i++)
				{
					System.out.println(i + ". username:" + rslist.get(i).get("username") + " sex:" + rslist.get(i).get("sex"));
				}
			}else{
				System.out.println("insert record error!");
			}
		}else{
			System.out.println("jtable create error!");
		}
	}else{
		System.out.println("jtest database create error!");
	}
}
//5.关闭连接
dbc.dbClose();
*********************************************/
