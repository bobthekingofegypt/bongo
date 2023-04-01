package org.bobstuff.bongo;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.bobstuff.bongo.executionstrategy.WriteExecutionConcurrentStrategy;
import org.bobstuff.bongo.executionstrategy.WriteExecutionSerialStrategy;
import org.bobstuff.bongo.executionstrategy.WriteExecutionStrategy;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

public class WriteExecutionStrategyProvider {

  private static List<WriteExecutionStrategyWrapper<?>> createOptions() {
    List<WriteExecutionStrategyWrapper<?>> options = new ArrayList<>();
    options.add(
        new WriteExecutionStrategyWrapper<>(
            "serial", new WriteExecutionSerialStrategy<>(), false, true));
    options.add(
        new WriteExecutionStrategyWrapper<>(
            "serial-unordered", new WriteExecutionSerialStrategy<>(), false, false));
    options.add(
        new WriteExecutionStrategyWrapper<>(
            "serial-compressed", new WriteExecutionSerialStrategy<>(), true, true));
    options.add(
        new WriteExecutionStrategyWrapper<>(
            "serial-compressed-unordered", new WriteExecutionSerialStrategy<>(), true, false));
    options.add(
        new WriteExecutionStrategyWrapper<>(
            "serial-compressed-unordered", new WriteExecutionSerialStrategy<>(), true, false));
    options.add(
        new WriteExecutionStrategyWrapper<>(
            "concurrent-low", new WriteExecutionConcurrentStrategy<>(1, 1), false, true));
    options.add(
        new WriteExecutionStrategyWrapper<>(
            "concurrent-low-unordered",
            new WriteExecutionConcurrentStrategy<>(1, 1),
            false,
            false));
    options.add(
        new WriteExecutionStrategyWrapper<>(
            "concurrent-low-compressed-unordered",
            new WriteExecutionConcurrentStrategy<>(1, 1),
            true,
            false));
    options.add(
        new WriteExecutionStrategyWrapper<>(
            "concurrent-low-compressed", new WriteExecutionConcurrentStrategy<>(1, 1), true, true));
    options.add(
        new WriteExecutionStrategyWrapper<>(
            "concurrent-high-unordered",
            new WriteExecutionConcurrentStrategy<>(5, 5),
            false,
            false));
    options.add(
        new WriteExecutionStrategyWrapper<>(
            "concurrent-high-unordered-compressed",
            new WriteExecutionConcurrentStrategy<>(5, 5),
            true,
            false));

    return options;
  }

  public static class WriteExecutionStrategyProviderAll implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
      return createOptions().stream()
          .map((option) -> Arguments.of(Named.named(option.getName(), option)));
    }
  }

  public static class WriteExecutionStrategyProviderOrdered implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
      return createOptions().stream()
          .filter(WriteExecutionStrategyWrapper::isOrdered)
          .map((option) -> Arguments.of(Named.named(option.getName(), option)));
    }
  }

  public static class WriteExecutionStrategyProviderUnordered implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
      return createOptions().stream()
          .filter(Predicate.not(WriteExecutionStrategyWrapper::isOrdered))
          .map((option) -> Arguments.of(Named.named(option.getName(), option)));
    }
  }

  public static class WriteExecutionStrategyWrapper<TModel> {
    private String name;
    private WriteExecutionStrategy<TModel> strategy;
    private BongoBulkWriteOptions options;
    private BongoInsertManyOptions insertManyOptions;
    private boolean compressed;
    private boolean ordered;

    public WriteExecutionStrategyWrapper(
        String name, WriteExecutionStrategy<TModel> strategy, boolean compressed, boolean ordered) {
      this.name = name;
      this.strategy = strategy;
      this.compressed = compressed;
      this.ordered = ordered;
      this.options = BongoBulkWriteOptions.builder().compress(compressed).ordered(ordered).build();
      this.insertManyOptions =
          BongoInsertManyOptions.builder().compress(compressed).ordered(ordered).build();
    }

    public String getName() {
      return name;
    }

    public boolean isCompressed() {
      return compressed;
    }

    public boolean isOrdered() {
      return ordered;
    }

    public WriteExecutionStrategy<TModel> getStrategy() {
      return strategy;
    }

    public void setStrategy(WriteExecutionStrategy<TModel> strategy) {
      this.strategy = strategy;
    }

    public BongoInsertManyOptions getInsertManyOptions() {
      return insertManyOptions;
    }

    public void setInsertManyOptions(BongoInsertManyOptions insertManyOptions) {
      this.insertManyOptions = insertManyOptions;
    }

    public BongoBulkWriteOptions getOptions() {
      return options;
    }

    public void setOptions(BongoBulkWriteOptions options) {
      this.options = options;
    }
  }
}
