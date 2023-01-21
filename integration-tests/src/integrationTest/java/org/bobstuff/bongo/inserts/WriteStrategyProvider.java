package org.bobstuff.bongo.inserts;

import java.util.stream.Stream;
import org.bobstuff.bongo.executionstrategy.WriteExecutionConcurrentStrategy;
import org.bobstuff.bongo.executionstrategy.WriteExecutionSerialStrategy;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

public class WriteStrategyProvider implements ArgumentsProvider {

  @Override
  public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
    return Stream.of(
        Arguments.of(new WriteExecutionSerialStrategy<>()),
        Arguments.of(new WriteExecutionConcurrentStrategy<>(3, 3)));
  }
}
