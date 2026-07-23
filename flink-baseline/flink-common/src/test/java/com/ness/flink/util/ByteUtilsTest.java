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

package com.ness.flink.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

class ByteUtilsTest {

    @Test
    void shouldConvertVarargsIntoTwoDimensionalArray() {
        byte[] first = "first".getBytes(StandardCharsets.UTF_8);
        byte[] second = "second".getBytes(StandardCharsets.UTF_8);

        byte[][] result = ByteUtils.convert(first, second);

        Assertions.assertEquals(2, result.length);
        Assertions.assertArrayEquals(first, result[0]);
        Assertions.assertArrayEquals(second, result[1]);
    }

    @Test
    void shouldReturnEmptyArrayWhenNoKeysProvided() {
        byte[][] result = ByteUtils.convert();

        Assertions.assertNotNull(result);
        Assertions.assertEquals(0, result.length);
    }

    @Test
    void shouldPreserveOrderAndReferences() {
        byte[] a = {1, 2, 3};
        byte[] b = {4, 5};
        byte[] c = {6};

        byte[][] result = ByteUtils.convert(a, b, c);

        Assertions.assertEquals(3, result.length);
        Assertions.assertSame(a, result[0]);
        Assertions.assertSame(b, result[1]);
        Assertions.assertSame(c, result[2]);
    }
}
