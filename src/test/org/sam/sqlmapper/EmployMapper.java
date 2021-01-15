package org.sam.sqlmapper;

public class EmployMapper extends SqlExecutor<Employee> {

    public EmployMapper(String driverName, String url, String id, String password) {
        super(driverName, url, id, password);
    }

}
