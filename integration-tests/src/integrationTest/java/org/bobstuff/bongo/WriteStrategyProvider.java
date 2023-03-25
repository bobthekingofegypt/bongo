package org.bobstuff.bongo;

import java.util.stream.Stream;
import org.bobstuff.bongo.executionstrategy.WriteExecutionConcurrentStrategy;
import org.bobstuff.bongo.executionstrategy.WriteExecutionSerialStrategy;
import org.bobstuff.bongo.executionstrategy.WriteExecutionStrategy;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

public class WriteStrategyProvider implements ArgumentsProvider {
  @Override
  public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
    return Stream.of(
        Arguments.of(
            Named.named(
                "serial",
                new WriteStrategyWrapper(
                    new WriteExecutionSerialStrategy(),
                    BongoInsertManyOptions.builder().ordered(true).compress(false).build()))),
        Arguments.of(
                Named.named(
                        "serial-compressed",
                        new WriteStrategyWrapper(
                                new WriteExecutionSerialStrategy(),
                                BongoInsertManyOptions.builder().ordered(true).compress(true).build()))),
        Arguments.of(
            Named.named(
                "concurrent-low",
                new WriteStrategyWrapper(
                    new WriteExecutionConcurrentStrategy(1, 1),
                    BongoInsertManyOptions.builder().ordered(true).compress(false).build()))),
      Arguments.of(
              Named.named(
                      "concurrent-low-compressed",
                      new WriteStrategyWrapper(
                              new WriteExecutionConcurrentStrategy(1, 1),
                              BongoInsertManyOptions.builder().ordered(true).compress(true).build()))));
  }

  public class WriteStrategyWrapper {
    private WriteExecutionStrategy strategy;
    private BongoInsertManyOptions options;

    public WriteStrategyWrapper(WriteExecutionStrategy strategy, BongoInsertManyOptions options) {
      this.strategy = strategy;
      this.options = options;
    }

    public WriteExecutionStrategy getStrategy() {
      return strategy;
    }

    public void setStrategy(WriteExecutionStrategy strategy) {
      this.strategy = strategy;
    }

    public BongoInsertManyOptions getOptions() {
      return options;
    }

    public void setOptions(BongoInsertManyOptions options) {
      this.options = options;
    }
  }
}
