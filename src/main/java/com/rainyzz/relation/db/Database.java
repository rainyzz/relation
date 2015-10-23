package com.rainyzz.relation.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Database {
	
	public Database(String dataBase,String username, String password){
		conn = getConnection(dataBase, username, password);
		try {
			st = (Statement) conn.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void closeConnection(){
		if (conn != null){
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	private Connection conn;
	private Statement st;

	
	public Connection getConnection(String dataBase, String username, String password) {
		Connection con = null;
		try {  
			Class.forName("com.mysql.jdbc.Driver");
			con = DriverManager.getConnection(  
					dataBase, username, password);
		} catch (Exception e) {  
			System.out.println("数据库连接失败" + e.getMessage());  
		}  
		return con;
	}
	public void executeUpdate(String sql){
		System.out.println("executeSQL >>>> "+sql);
		try {
			st.execute(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		} 
	}
	
	
	public ResultSet executeSQL(String sql){
		System.out.println("executeSQL >>>> "+sql);
		try {
			return st.executeQuery(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		} 
		return null;
	}
	


}
