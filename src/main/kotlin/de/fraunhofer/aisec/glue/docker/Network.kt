/*
 * Copyright 2024 Fraunhofer AISEC
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
package de.fraunhofer.aisec.glue.docker

import de.fraunhofer.aisec.glue.pipeline.IPv4Address
import de.fraunhofer.aisec.glue.pipeline.IPv4Network
import io.ktor.client.call.*
import io.ktor.client.request.*
import io.ktor.http.*
import de.fraunhofer.aisec.glue.docker.model.Network as NetworkInspectResult

class Network internal constructor(
    private val client: DockerClient,
    val name: String,
) {
    suspend fun inspect(): NetworkInspectResult {
        val resp = client.httpClient.get(client.endpoint("networks/$name"))
        return resp.body<NetworkInspectResult>()
    }

    suspend fun connect(
        container: Container,
        address: String? = null,
        aliases: List<String>? = null,
    ) {
        val endpointConfig =
            mutableMapOf<String, Any>(
                "IPAMConfig" to mapOf("IPv4Address" to address),
            )
        if (aliases != null) {
            endpointConfig["Aliases"] = aliases
        }
        val args =
            mapOf(
                "Container" to container.name,
                "EndpointConfig" to endpointConfig,
            )

        client.httpClient.post(client.endpoint("networks/$name/connect")) {
            setBody(args)
            contentType(ContentType.Application.Json)
        }
    }

    suspend fun disconnect(
        container: Container,
        force: Boolean = false,
    ) {
        val args =
            mapOf(
                "Container" to container.name,
                "Force" to force,
            )

        client.httpClient.post(client.endpoint("networks/$name/disconnect")) {
            setBody(args)
            contentType(ContentType.Application.Json)
        }
    }

    suspend fun remove() {
        client.httpClient.delete(client.endpoint("networks/$name"))
    }

    companion object {
        // We need 2 bits for the network, broadcast addresses and 2 container addresses.
        // Docker also assigns an address to the host, which means we need another bit.
        private const val LINK_NET_LEN = 29
        private val LINK_NAMES = generateSequence(0, Int::inc).map { i -> "link-$i" }.iterator()
        private val LINK_NETS = IPv4Network(IPv4Address.parse("10.1.0.0"), 16).subnets(LINK_NET_LEN).iterator()

        suspend fun link(
            client: DockerClient,
            from: Container,
            to: Container,
        ): Network {
            val net = client.networks.create(LINK_NAMES.next(), LINK_NETS.next().toString())
            net.connect(from)
            net.connect(to)
            return net
        }
    }
}
