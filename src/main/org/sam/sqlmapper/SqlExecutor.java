package org.sam.sqlmapper;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.*;
import java.util.*;

public abstract class SqlExecutor<T> {

    private final String url;
    private final String id;
    private final String password;

    private final Class<T> type;

    public SqlExecutor(String driverName, String url, String id, String password) {
        this.type = getGenericType();
        this.url = url;
        this.id = id;
        this.password = password;

        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("unchecked")
    private Class<T> getGenericType() {
        Type genericSuperClass = getClass().getGenericSuperclass();
        if (!(genericSuperClass instanceof ParameterizedType)) {
            throw new RuntimeException();
        }
        ParameterizedType parameterizedType = (ParameterizedType) genericSuperClass;
        Type actualTypeArgument = parameterizedType.getActualTypeArguments()[0];
        return (Class<T>) actualTypeArgument;
    }

    public void execute(String sql) {
        try (Connection conn = DriverManager.getConnection(url, id, password)) {
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public int insert(String sql, Object... args) {
        try (Connection conn = DriverManager.getConnection(url, id, password)) {
            PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            setPreparedStatement(ps, args);
            ps.executeUpdate();
            ResultSet rs = ps.getGeneratedKeys();
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public List<T> selectList(String sql, Object... args) {
        List<T> list = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(url, id, password)) {
            List<Method> methods = Arrays.asList(type.getMethods());
            PreparedStatement ps = conn.prepareStatement(sql);
            ResultSet rs = getResultSet(ps, args);
            ResultSetMetaData metaData = rs.getMetaData();
            while (rs.next()) {
                T instance = createInstance(methods, rs, metaData);
                list.add(instance);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    public T selectOne(String sql, Object... args) {
        try (Connection conn = DriverManager.getConnection(url, id, password)) {
            List<Method> methods = Arrays.asList(type.getMethods());
            PreparedStatement ps = conn.prepareStatement(sql);
            ResultSet rs = getResultSet(ps, args);
            ResultSetMetaData metaData = rs.getMetaData();
            if (rs.next()) {
                return createInstance(methods, rs, metaData);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public int updateOrDelete(String sql, Object... args) {
        try (Connection conn = DriverManager.getConnection(url, id, password)) {
            PreparedStatement ps = conn.prepareStatement(sql);
            setPreparedStatement(ps, args);
            return ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public int selectCount(String sql, Object... args) {
        try (Connection conn = DriverManager.getConnection(url, id, password)) {
            PreparedStatement ps = conn.prepareStatement(sql);
            ResultSet rs = getResultSet(ps, args);
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    private ResultSet getResultSet(PreparedStatement ps, Object[] args) throws SQLException {
        setPreparedStatement(ps, args);
        return ps.executeQuery();
    }

    private void setPreparedStatement(PreparedStatement ps, Object[] args) throws SQLException {
        for (int i = 1; i <= args.length; i++) {
            ps.setObject(i, args[i - 1]);
        }
    }

    private T createInstance(List<Method> methods, ResultSet rs, ResultSetMetaData metaData) {
        T instance = null;
        try {
            instance = type.newInstance();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                String setterName = getSetterName(columnName);
                Object value = rs.getObject(columnName);
                Optional<Method> optionalSetter = methods.stream()
                        .filter(method -> method.getName().equals(setterName))
                        .findAny();
                if (!optionalSetter.isPresent()) {
                    continue;
                }

                Method setter = optionalSetter.get();
                Class<?> parameterType = Objects.requireNonNull(setter).getParameterTypes()[0];
                if (value instanceof Number && Number.class.isAssignableFrom(parameterType)) {
                    Method method = parameterType.getMethod("parse" + parameterType.getSimpleName(), String.class);
                    value = method.invoke(null, String.valueOf(value));
                }
                setter.invoke(instance, parameterType.cast(value));
            }
        } catch (InstantiationException | IllegalAccessException | SQLException | InvocationTargetException | NoSuchMethodException e) {
            e.printStackTrace();
        }

        return instance;
    }

    private String getSetterName(String columnName) {
        String[] columnNames = columnName.split("_");
        StringBuilder sb = new StringBuilder();
        for (String name : columnNames) {
            sb.append(name.substring(0, 1).toUpperCase()).append(name.substring(1));
        }
        return "set" + sb.toString();
    }

}

