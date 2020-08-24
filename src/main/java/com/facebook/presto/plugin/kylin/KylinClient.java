package com.facebook.presto.plugin.kylin;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.plugin.jdbc.*;
import com.google.common.base.Joiner;
import org.apache.kylin.jdbc.Driver;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class KylinClient
        extends BaseJdbcClient
{
    private static final Joiner DOT_JOINER = Joiner.on(".");
    private static final Logger log = Logger.get(BaseJdbcClient.class);

    @Inject
    public KylinClient(JdbcConnectorId connectorId, BaseJdbcConfig config) {
        super(connectorId, config, "\"", new DriverConnectionFactory(new Driver(), config));
        log.info("Creating a kylin Client!");
    }

    @Override
    public PreparedStatement buildSql(Connection connection, JdbcSplit split, List<JdbcColumnHandle> columnHandles)
            throws SQLException
    {
        return new KylinQueryBuilder(identifierQuote).buildSql(
                this,
                connection,
                split.getCatalogName(),
                split.getSchemaName(),
                split.getTableName(),
                columnHandles,
                split.getTupleDomain(),
                split.getAdditionalPredicate());
    }

    private static String singleQuote(String literal)
    {
        return "\'" + literal + "\'";
    }
}
