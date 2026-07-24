/*
 * Copyright 2021-2023 Ness Digital Engineering
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ness.flink.config.properties;

import org.apache.kafka.common.config.SaslConfigs;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

/**
 * Verifies that {@link KafkaProperties#redactForLogging} masks credential-bearing keys
 * regardless of whether the credential was injected from a secret manager or provided inline —
 * the security fix preventing plaintext SASL/JAAS credentials from being logged at INFO.
 */
class KafkaPropertiesRedactionTest {

    @Test
    void shouldMaskInlineCredentialsAndPasswords() {
        Properties props = new Properties();
        props.put(SaslConfigs.SASL_JAAS_CONFIG,
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
                + "username='API_KEY' password='super-secret';");
        props.put("ssl.truststore.password", "trust-pw");
        props.put("basic.auth.user.info", "user:pass");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "priceSmoothing");

        Map<Object, Object> redacted = KafkaProperties.redactForLogging(props);

        Assertions.assertEquals("***", redacted.get(SaslConfigs.SASL_JAAS_CONFIG));
        Assertions.assertEquals("***", redacted.get("ssl.truststore.password"));
        Assertions.assertEquals("***", redacted.get("basic.auth.user.info"));
        // Non-sensitive values pass through unchanged
        Assertions.assertEquals("localhost:9092", redacted.get("bootstrap.servers"));
        Assertions.assertEquals("priceSmoothing", redacted.get("group.id"));
    }

    @Test
    void shouldNotMutateOriginalProperties() {
        Properties props = new Properties();
        props.put(SaslConfigs.SASL_JAAS_CONFIG, "module required password='keep-me';");

        KafkaProperties.redactForLogging(props);

        // The real properties handed to Kafka must still contain the actual credential.
        Assertions.assertTrue(props.getProperty(SaslConfigs.SASL_JAAS_CONFIG).contains("keep-me"));
    }
}
