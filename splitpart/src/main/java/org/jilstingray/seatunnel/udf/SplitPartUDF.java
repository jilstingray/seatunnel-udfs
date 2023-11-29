/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jilstingray.seatunnel.udf;

import com.google.auto.service.AutoService;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.transform.sql.zeta.ZetaUDF;

import java.util.List;

@AutoService(ZetaUDF.class)
public class SplitPartUDF implements ZetaUDF {

    @Override
    public String functionName() {
        return "SPLIT_PART";
    }

    @Override
    public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType) {
        return argsType.get(argsType.size() - 1);
    }

    /**
     * SPLIT_PART
     * @param args string, delimiter, index
     * @return value
     */
    @Override
    public Object evaluate(List<Object> args) {
        if (args.size() != 3) {
            throw new IllegalArgumentException("SPLIT_PART() must have 3 arguments");
        }
        Object field = args.get(0);
        String delimiter = (String) args.get(1);
        int index = (int) args.get(2);
        String[] split = ((String) field).split(delimiter, -1);
        if (index < 0 || index >= split.length) {
            throw new IllegalArgumentException("SPLIT_PART() index out of bounds");
        }
        return split[index];
    }
}