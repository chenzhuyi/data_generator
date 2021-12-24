package tools.springdatagenerator.beans;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RuntimeConfig {

    @Value("${runtime.parallel.threads}")
    public int threads;

    @Value("${runtime.tablename}")
    public String tableList;

    @Value("${runtime.delete.onstart}")
    public boolean needTruncate;

    @Value("${runtime.delete.threshold}")
    public int deleteThreshold;

    @Value("${runtime.max.insert.count.perthread}")
    public int maxInsertCountPerThread;

    @Value("${runtime.max.transaction.size}")
    public int maxTransactionSize;

    @Value("${runtime.max.update.count.perthread}")
    public int maxUpdateCountPerThread;

    @Value("${runtime.specify.values}")
    public String specifyValues;

    @Value("${runtime.max.try.round}")
    public int maxTryRound;
}
