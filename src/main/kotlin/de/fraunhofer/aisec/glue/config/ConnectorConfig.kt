/*
 * Copyright 2025-2026 Fraunhofer AISEC
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
package de.fraunhofer.aisec.glue.config

import io.ktor.http.*
import io.ktor.server.config.*
import java.net.InetAddress

/**
 * Configuration for the connector itself.
 */
class ConnectorConfig(
    inner: ApplicationConfig,
) : Config(inner) {
    /**
     * The identity of this connector that is used as assigner and assignee during contract negotiation.
     */
    val identity by required(stringProp())

    /**
     * The default public URL to use for callbacks when starting a negotiation.
     * When this property is not set, the local IP address is used, which will likely not be routable in production.
     */
    val publicUrl by withDefault(
        convert(
            stringProp(),
            ::Url,
        ),
        URLBuilder(host = InetAddress.getLocalHost().hostAddress, port = 8080).build(),
    )

    /**
     * Public DNS domain name used for SNI-based routing of (A)TLS traffic to pipelines.
     * When set, pipeline entry containers are assigned hostnames of the form
     * `<pipelineId>.<publicDomain>` and connected to the Envoy mux network so
     * that Envoy's SNI dynamic forward proxy can route TLS connections to them
     * on a single port. When unset, the legacy random-ephemeral-port behaviour
     * is used.
     */
    val publicDomain by stringProp()

    enum class EdcMode {
        NONE,
        COMPAT,
    }

    val edcMode by withDefault(
        convert(stringProp()) { EdcMode.valueOf(it.uppercase()) },
        EdcMode.COMPAT,
    )
}
