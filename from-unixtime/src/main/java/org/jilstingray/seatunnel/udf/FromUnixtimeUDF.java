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
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.transform.sql.zeta.ZetaUDF;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

@AutoService(ZetaUDF.class)
public class FromUnixtimeUDF implements ZetaUDF {
    @Override
    public String functionName() {
        return "FROM_UNIXTIME";
    }

    @Override
    public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public Object evaluate(List<Object> args) {
        if (args.size() != 1) {
            throw new IllegalArgumentException("FROM_UNIXTIME() requires exactly one argument");
        }
        long timestamp = (long) args.get(0);
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(timestamp * 1000));
    }
}