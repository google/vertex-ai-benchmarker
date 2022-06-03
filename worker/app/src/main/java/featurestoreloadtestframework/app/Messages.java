/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package featurestoreloadtestframework.app;

import java.text.MessageFormat;
import java.util.Locale;
import java.util.ResourceBundle;
import org.kohsuke.args4j.Localizable;

/**
 * Messages class which gets messages from the .properties file.
 * Not thread safe.
 */
public enum Messages implements Localizable {
  NO_ARGUMENT_GIVEN,
  ;

  private static ResourceBundle bundle = null;

  /**
   * Get the resource bundle with a certain locale.
   * @param locale The locale to use when getting the resource bundle.
   * @return The localized resource bundle.
   */
  private static ResourceBundle getBundle(Locale locale) {
    if (bundle == null) {
      bundle = ResourceBundle.getBundle("featurestoreloadtestframework.app.Messages", locale);
      assert(bundle != null);
    }
    return bundle;
  }

  private String getMessage() {
    return getMessage(Locale.getDefault());
  }

  /**
   * @return The literal string from the resource bundle.
   */
  private String getMessage(Locale locale) {
    ResourceBundle bundle = getBundle(locale);
    String msgKey = this.toString();
    return bundle.getString(msgKey);
  }

  @Override
  public String format(Object... args) {
    return MessageFormat.format(getMessage(), args);
  }

  @Override
  public String formatWithLocale(Locale locale, Object... args) {
    return MessageFormat.format(getMessage(locale), args);
  }
}
