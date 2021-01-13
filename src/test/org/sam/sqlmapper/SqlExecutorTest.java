package org.sam.sqlmapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SqlExecutorTest {

    SqlExecutor<Employee> sqlExecutor;

    @BeforeEach
    void init() {
        final String driverName = "org.h2.Driver";
        final String url = "jdbc:h2:~/test";
        final String id = "sa";
        final String password = "";
        this.sqlExecutor = new SqlExecutor<Employee>(driverName, url, id, password) {};

        String sql = "CREATE TABLE If Not Exists `employee`" +
                "( " +
                "seq int auto_increment, " +
                "name varchar(50) not null, " +
                "age int not null, " +
                "reg_date datetime null, " +
                "constraint employee_seq_uindex " +
                "unique (seq) " +
                ")";
        sqlExecutor.execute(sql);
    }

    @Test
    void insert_test() {
        String sql = "INSERT INTO employee (name, age, reg_date) VALUES (?, ?, now())";
        int seq = sqlExecutor.insert(sql, "sam", 31);
        assertNotEquals(-1, seq);
    }

}