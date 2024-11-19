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
import java.util.Objects;

@AutoService(ZetaUDF.class)
public class CaseWhenUDF implements ZetaUDF {

    @Override
    public String functionName() {
        return "CASEWHEN";
    }

    @Override
    public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType) {
        return argsType.get(argsType.size() - 1);
    }

    /**
     * CASE WHEN clause
     * @param args field, condition, value, condition, value, ..., else
     * @return value
     */
    @Override
    public Object evaluate(List<Object> args) {
        if (args.size() < 4) {
            throw new IllegalArgumentException("CASEWHEN() must have at least 4 arguments");
        }
        if (args.size() % 2 != 0) {
            throw new IllegalArgumentException("CASEWHEN() must have even number of arguments");
        }
        Object field = args.get(0);
        for (int i = 1; i < args.size() - 1; i += 2) {
            Object condition = args.get(i);
            Object value = args.get(i + 1);
            if (Objects.equals(field, condition)) {
                return value;
            }
        }
        return args.get(args.size() - 1);
    }
}