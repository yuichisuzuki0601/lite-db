package jp.co.sbro.lite_db;

import java.io.IOException;
import java.util.Date;

import org.junit.Test;

public class TableTest {

	@Test
	public void test() throws IOException {
		Table t = Table.get("db/define/sample.json");
		t.drop();
		t.create();
		t.insert(1, "hoge", new Date());
		t.insert(2, "fuga", new Date());
		t.insert(3, "piyo", new Date());
		System.out.println(t.select());
	}

}
