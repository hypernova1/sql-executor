package org.sam.sqlmapper;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public abstract class SqlExecutor<T> {

    private final String url;
    private final String id;
    private final String password;

    private final Class<T> type;

    @SuppressWarnings("unchecked")
    public SqlExecutor(String driverName, String url, String id, String password) {
        this.url = url;
        this.id = id;
        this.password = password;

        Type sType = getClass().getGenericSuperclass();
        if (sType instanceof ParameterizedType) {
            this.type = (Class<T>) ((ParameterizedType) sType).getActualTypeArguments()[0];
        } else {
            throw new RuntimeException();
        }

        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public int insert(String sql, Object... args) {
        try (Connection conn = DriverManager.getConnection(url, id, password)) {
            PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            for (int i = 1; i <= args.length; i++) {
                ps.setObject(i, args[i - 1]);
            }
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
            PreparedStatement ps = conn.prepareStatement(sql);
            for (int i = 1; i <= args.length; i++) {
                ps.setObject(i, args[i - 1]);
            }
            List<Method> methods = Arrays.asList(type.getMethods());
            ResultSet rs = ps.executeQuery();
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
            PreparedStatement ps = conn.prepareStatement(sql);
            for (int i = 1; i <= args.length; i++) {
                ps.setObject(i, args[i - 1]);
            }
            List<Method> methods = Arrays.asList(type.getMethods());
            ResultSet rs = ps.executeQuery();
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
            for (int i = 1; i <= args.length; i++) {
                ps.setObject(i, args[i - 1]);
            }
            return ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public int selectCount(String sql, Object... args) {
        try (Connection conn = DriverManager.getConnection(url, id, password)) {
            PreparedStatement ps = conn.prepareStatement(sql);
            for (int i = 1; i <= args.length; i++) {
                ps.setObject(i, args[i - 1]);
            }
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    private T createInstance(List<Method> methods, ResultSet rs, ResultSetMetaData metaData) {
        T instance = null;
        try {
            instance = type.newInstance();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                String setterName = getSetterName(columnName);
                Object value = rs.getObject(columnName);
                Method setter = methods.stream().filter(method -> method.getName().equals(setterName))
                        .findAny()
                        .orElse(null);
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

