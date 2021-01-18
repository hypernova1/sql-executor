package org.sam.sqlmapper;

import java.util.List;

public interface Executor<T> {

    void execute(String sql);

    int insert(String sql, Object... args);

    T selectOne(String sql, Object... args);

    List<T> selectList(String sql, Object... args);

    int updateOrDelete(String sql, Object... args);

    int selectCount(String sql, Object... args);

}
