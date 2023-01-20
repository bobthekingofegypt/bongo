package org.bobstuff.bongo;

import de.flapdoodle.embed.mongo.commands.MongodArguments;
import de.flapdoodle.embed.mongo.commands.ServerAddress;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.mongo.transitions.Mongod;
import de.flapdoodle.embed.mongo.transitions.RunningMongodProcess;
import de.flapdoodle.reverse.StateID;
import de.flapdoodle.reverse.TransitionWalker;
import de.flapdoodle.reverse.transitions.Start;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;

public class MongoDbResolver implements ParameterResolver, AfterEachCallback {
  private TransitionWalker.ReachedState<RunningMongodProcess> process;
  private ServerAddress mongoUrl;

  @Override
  public void afterEach(ExtensionContext testExtensionContext) throws Exception {
    if (process == null) {
      return;
    }

    process.close();
    process = null;
  }

  @Override
  public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext context) {
    return parameterContext.getParameter().isAnnotationPresent(MongoDbDatabase.class)
        || parameterContext.getParameter().isAnnotationPresent(MongoUrl.class);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext) {
    if (process == null) {
      try {
        process =
            Mongod.instance()
                .transitions(Version.Main.V6_0)
                .replace(
                    Start.to(MongodArguments.class)
                        .initializedWith(MongodArguments.defaults().withIsConfigServer(false)))
                .walker()
                .initState(StateID.of(RunningMongodProcess.class));

        mongoUrl = process.current().getServerAddress();
      } catch (Exception e) {
        throw new RuntimeException("Failed to initialise mongodb server");
      }
    }

    if (parameterContext.getParameter().getAnnotation(MongoUrl.class) != null) {
      return mongoUrl;
    }

    return null;
  }
}
