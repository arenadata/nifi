package org.apache.nifi.tests.system.arenadata.service;

import io.qameta.allure.Allure;
import io.qameta.allure.Step;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class JdbcService {

    private static final int DEFAULT_RECONNECT_TIMEOUT = 60;
    private static final int DEFAULT_RECONNECT_POLL_INTERVAL = 5;

    private static final Logger logger = LoggerFactory.getLogger(JdbcService.class);
    protected final JdbcTemplate jdbcTemplate;

    public JdbcService(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Step("Execute sql")
    public void exec(String sql) {
        exec(sql, false);
    }

    @Step("Execute sql with reconnection on fail {reconnectOnFail}")
    public void exec(String sql, boolean reconnectOnFail) {
        if (reconnectOnFail) {
            executeWithReconnection(() -> jdbcTemplate.execute(sql));
        } else {
            jdbcTemplate.execute(sql);
        }
    }

    @Step("Get result list by query")
    public List<Map<String, Object>> queryForList(String sql) {
        return queryForList(sql, false);
    }

    @Step("Get result list by query with reconnection on fail {reconnectOnFail}")
    public List<Map<String, Object>> queryForList(String sql, boolean reconnectOnFail) {
        List<Map<String, Object>> resultList = reconnectOnFail ?
                executeWithReconnection(CompletableFuture.supplyAsync(() -> jdbcTemplate.queryForList(sql))) :
                jdbcTemplate.queryForList(sql);
        StringBuilder resultText = new StringBuilder("Query Result:\n");
        for (Map<String, Object> row : resultList) {
            resultText.append(row.toString()).append("\n");
        }
        Allure.addAttachment("Query Result", resultText.toString());
        return resultList;
    }

    @Step("Select count() the table {tableName}")
    public int queryCountOfRowsInTable(String tableName) {
        Integer result = jdbcTemplate.queryForObject(
                String.format("SELECT count(*) FROM %s", tableName),
                Integer.class);
        return Optional.ofNullable(result).orElse(0);
    }

    private void executeWithReconnection(Runnable jdbcRunnable) {
        executeWithReconnection(CompletableFuture.runAsync(jdbcRunnable));
    }

    private <V> V executeWithReconnection(Future<V> jdbcFuture) {
        AtomicReference<V> result = new AtomicReference<>();
        Awaitility.waitAtMost(Duration.of(DEFAULT_RECONNECT_TIMEOUT, ChronoUnit.SECONDS))
                .pollInterval(DEFAULT_RECONNECT_POLL_INTERVAL, TimeUnit.SECONDS)
                .until(() -> {
                    try {
                        result.set(jdbcFuture.get());
                        return true;
                    } catch (Exception e) {
                        logger.warn("Exception occurred while executing JDBC future", e);
                        return false;
                    }
                });
        return result.get();
    }
}
