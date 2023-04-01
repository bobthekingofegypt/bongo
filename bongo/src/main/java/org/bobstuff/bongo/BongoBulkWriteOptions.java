package org.bobstuff.bongo;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class BongoBulkWriteOptions {
  private @Builder.Default boolean ordered = true;
  private Boolean bypassDocumentValidation;
  private String comment;
  private Boolean compress;

  public static BongoBulkWriteOptions from(BongoUpdateOptions options) {
    return BongoBulkWriteOptions.builder()
        .bypassDocumentValidation(options.getBypassDocumentValidation())
        .comment(options.getComment())
        .build();
  }

  public static BongoBulkWriteOptions from(BongoDeleteOptions options) {
    return BongoBulkWriteOptions.builder()
        .bypassDocumentValidation(options.getBypassDocumentValidation())
        .comment(options.getComment())
        .build();
  }

  public static BongoBulkWriteOptions from(BongoInsertOneOptions options) {
    return BongoBulkWriteOptions.builder()
        .bypassDocumentValidation(options.getBypassDocumentValidation())
        .comment(options.getComment())
        .build();
  }

  public static BongoBulkWriteOptions from(BongoInsertManyOptions options) {
    return BongoBulkWriteOptions.builder()
        .bypassDocumentValidation(options.getBypassDocumentValidation())
        .comment(options.getComment())
        .ordered(options.isOrdered())
        .build();
  }
}
