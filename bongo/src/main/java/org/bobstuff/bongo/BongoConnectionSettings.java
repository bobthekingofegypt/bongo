package org.bobstuff.bongo;

import java.util.List;
import lombok.Builder;
import lombok.Data;
import lombok.Singular;
import org.bobstuff.bongo.auth.BongoCredentials;
import org.bobstuff.bongo.compressors.BongoCompressor;

@Builder
@Data
public class BongoConnectionSettings {
  @Singular private final List<String> hosts;
  @Singular private final List<BongoCompressor> compressors;
  private final BongoCredentials credentials;
}
