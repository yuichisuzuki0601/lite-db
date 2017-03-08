package jp.co.sbro.lite_db;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.fasterxml.jackson.databind.ObjectMapper;

public class Table {

	public static class Column {
		private boolean isKey;
		private String name;
		private String type;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getType() {
			return type;
		}

		public void setType(String type) {
			this.isKey = type.contains("PRIMARY KEY");
			this.type = type;
		}
	}

	private static String url = "jdbc:h2:./db/data";
	private static String user = "sa";
	private static String password = "";

	private static JdbcTemplate j;

	public static Table get(String path) throws IOException {
		Table t = new ObjectMapper().readValue(new File(path), Table.class);
		if (j == null) {
			DriverManagerDataSource ds = new DriverManagerDataSource();
			ds.setUrl(url);
			ds.setUsername(user);
			ds.setPassword(password);
			j = new JdbcTemplate(ds, true);
		}
		return t;
	}

	private String name;
	private List<Column> columns;

	private Table() {
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<Column> getColumns() {
		return columns;
	}

	public void setColumn(List<Column> columns) {
		this.columns = columns;
	}

	private Column getKeyColumn() {
		return columns.stream().filter(c -> c.isKey).findFirst().get();
	}

	public boolean exists() {
		return j.query(" show tables ", new RowMapper<String>() {
			@Override
			public String mapRow(ResultSet rs, int rowNum) throws SQLException {
				return rs.getString("TABLE_NAME");
			}
		}).contains(name.toUpperCase());
	}

	public void drop() {
		j.execute(" drop table if exists " + name);
	}

	public void create() {
		j.execute(new StringBuilder(" create table if not exists ").append(name).append(" ( ").append(
				columns.stream().map(c -> c.getName() + " " + c.getType()).reduce((s1, s2) -> s1 + ", " + s2).get())
				.append(" ) ").toString());
	}

	public void truncate() {
		j.execute(" truncate table " + name);
	}

	public int insert(Object... values) {
		return j.update(new StringBuilder(" insert into ").append(name).append(" values ( ")
				.append(Arrays.stream(values).map(v -> "?").reduce((v1, v2) -> v1 + ", " + v2).get()).append(" ) ")
				.toString(), values);
	}

	public int insertIfNotExists(Object... values) {
		int result = 0;
		if (select(getKeyColumn().name, values[0]).isEmpty()) {
			result = insert(values);
		}
		return result;
	}

	public List<Map<String, Object>> select() {
		return select(null);
	}

	public List<Map<String, Object>> select(String column, Object value) {
		Map<String, Object> cond = new HashMap<>();
		cond.put(column, value);
		return select(cond);
	}

	public List<Map<String, Object>> select(Map<String, Object> cond) {
		StringBuilder sql = new StringBuilder(" select ")
				.append(columns.stream().map(c -> c.getName()).reduce((s1, s2) -> s1 + ", " + s2).get())
				.append(" from ").append(name);
		if (cond != null) {
			sql.append(" where ").append(cond.entrySet().stream().map(c -> c.getKey() + " = '" + c.getValue() + "' ")
					.reduce((c1, c2) -> c1 + " AND " + c2).get());
		}
		sql.append(" order by 1 ");
		return j.query(sql.toString(), new ColumnMapRowMapper());
	}

	public int update(Object key, String column, Object value) {
		Map<String, Object> cond = new HashMap<>();
		cond.put(column, value);
		return update(key, cond);
	}

	public int update(Object key, Map<String, Object> data) {
		return j.update(new StringBuilder(" update ").append(name).append(" set ")
				.append(data.entrySet().stream().map(d -> d.getKey() + " = '" + d.getValue() + "'")
						.reduce((d1, d2) -> d1 + ", " + d2).get())
				.append(" where ").append(getKeyColumn().name).append(" = '").append(key).append("'").toString());
	}

	public int delete(Object key) {
		return j.update(new StringBuilder(" delete from ").append(name).append(" where ").append(getKeyColumn().name)
				.append(" = '").append(key).append("'").toString());
	}

}
