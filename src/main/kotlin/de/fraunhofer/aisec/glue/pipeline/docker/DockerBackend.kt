/*
 * Copyright 2024-2026 Fraunhofer AISEC
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
package de.fraunhofer.aisec.glue.pipeline.docker

import com.fasterxml.jackson.databind.ObjectMapper
import de.fraunhofer.aisec.glue.config.Config
import de.fraunhofer.aisec.glue.config.ConfigManager
import de.fraunhofer.aisec.glue.config.ConnectorConfig
import de.fraunhofer.aisec.glue.docker.Container
import de.fraunhofer.aisec.glue.docker.DockerClient
import de.fraunhofer.aisec.glue.docker.Network
import de.fraunhofer.aisec.glue.docker.model.*
import de.fraunhofer.aisec.glue.pipeline.*
import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.server.config.*
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import java.net.InetAddress
import java.util.concurrent.ConcurrentLinkedQueue

class DockerBackend(
    private val objectMapper: ObjectMapper,
    private val dockerClient: DockerClient,
    configManager: ConfigManager,
) : Backend {
    private val dockerSubnet = System.getenv("DOCKER_SUBNET") ?: "10.1.0.0"
    private val subnetLen = 29
    private val subnets =
        IPv4Network(IPv4Address.parse(dockerSubnet), 16)
            .subnets(subnetLen)
            .iterator()
    private val config = configManager.getConfig(::DockerConfig)
    private val connectorConfig = configManager.getConfig(::ConnectorConfig)

    // Note: There is no reliable way to determine if running in a container.
    // Checking for .dockerenv is an implementation detail and the hostname can be overwritten.
    // Even when we use such implementation details, we cannot tell if we are running on the same docker engine as the
    // container pipelines (the one whose API socket the supplied docker client is connected to).
    // As a result, we currently do not support running inside a sibling container with the docker backend while keeping
    // this code around to make eventual adaptation easy.
    private val selfContainer: Container? by lazy {
        null
    }

    private suspend fun <T, R> mapParallel(
        input: List<T>,
        close: suspend (R) -> Unit,
        map: suspend (Int, T) -> R,
    ): List<R> {
        val completed = ConcurrentLinkedQueue<R & Any>()
        try {
            return coroutineScope {
                input.mapIndexed { index, item -> async { map(index, item).also(completed::add) } }.awaitAll()
            }
        } catch (t: Throwable) {
            completed.forEach { runCatching { close(it) }.onFailure(t::addSuppressed) }
            throw t
        }
    }

    private suspend fun convertPipeline(
        id: String,
        pipelineSpec: List<AugmentedGlueContainer>,
        contractId: String,
        targetId: String,
    ): MutableList<ContainerCreateRequest> {
        checkHashes(pipelineSpec)
        return pipelineSpec
            .map { c ->
                //       We probably should persist this to the point where we inject the config
                ContainerCreateRequest(
                    image = c.base.image,
                    hostConfig =
                        HostConfig(
                            networkMode = if (c.base.allowNetwork == true) "bridge" else "none",
                            binds = c.extraVolumes.map { (hostPath, containerPath) -> "$hostPath:$containerPath" },
                        ),
                    volumes = c.extraVolumes.map { (_, containerPath) -> containerPath to object {} }.toMap(),
                    // Attach labels to the containers so we can recover running pipelines if we crash and/or restart
                    labels =
                        mapOf(
                            "de.fraunhofer.aisec.glue.pipeline.id" to id,
                            "de.fraunhofer.aisec.glue.pipeline.contract" to contractId,
                            "de.fraunhofer.aisec.glue.pipeline.target" to targetId,
                        ),
                )
            }.toMutableList()
    }

    // Hash check adapted from https://github.com/Fraunhofer-AISEC/trusted-connector/blob/16fb97953c89e796d80c52cacc7a4445c1dda019/ids-dataflow-control/src/main/kotlin/de/fhg/aisec/ids/dataflowcontrol/usagecontrol/DockerImageConstraint.kt#L104
    private suspend fun checkHashes(pipelineDef: List<AugmentedGlueContainer>) {
        for (augmentedGlueContainer in pipelineDef) {
            val glueContainer = augmentedGlueContainer.base
            val expectedHash = glueContainer.hash
            val imageToCheck = dockerClient.images.get(glueContainer.image)
            val inspectResponse = imageToCheck.inspect()
            val idHash = inspectResponse.id
            val repoDigests = inspectResponse.repoDigests?.map { it.split("@").last() }

            if (expectedHash == idHash || repoDigests?.any { expectedHash == it } == true) {
                LOG.atTrace {
                    message = "Container hash check passed"
                    payload =
                        mapOf(
                            "image" to glueContainer.image,
                            "expectedHash" to expectedHash,
                            "idHash" to idHash,
                            "repoDigests" to repoDigests?.joinToString(", "),
                        )
                }
                continue
            }

            LOG.atWarn {
                message = "Hash check failed."
                payload =
                    mapOf(
                        "image" to glueContainer.image,
                        "expectedDigest" to glueContainer.hash,
                        "configDigest" to idHash,
                        "repoDigests" to repoDigests,
                    )
            }

            throw Exception(
                "The hash check for container ${glueContainer.image} failed. " +
                    "Expected $expectedHash, found candidates:\nID Hash: $idHash, " +
                    "Repo Digests: ${repoDigests?.joinToString(", ") ?: "none"}",
            )
        }
    }

    private suspend fun createContainers(
        id: String,
        containerSpecs: List<ContainerCreateRequest>,
    ): List<Container> =
        mapParallel(containerSpecs, Container::remove) { index, item ->
            dockerClient.containers.create("pipeline-$id-$index", item)
        }

    private suspend fun linkContainers(
        id: String,
        containers: List<Container>,
        contractId: String,
        targetId: String,
    ): List<Network> {
        // windowed(2) yields no pairs for a single container, so use that as minimum to always
        // create at least one network (connecting the sole container and the gateway/controller).
        val containerPairs =
            if (containers.size == 1) {
                listOf(containers)
            } else {
                containers.windowed(2)
            }

        // Pre-allocate subnets sequentially — the iterator is not thread-safe
        val linkRequests =
            containerPairs.mapIndexed { i, c ->
                val subnet = subnets.next()
                val subnetCidr = subnet.toString()
                val gateway = subnet.host(1u).toString()
                NetworkCreateRequest(
                    name = "pipeline-$id-net-$i",
                    ipam = IPAM(config = listOf(IPAMConfig(subnet = subnetCidr, gateway = gateway))),
                    internal = true,
                    labels =
                        mapOf(
                            "de.fraunhofer.aisec.glue.pipeline.id" to id,
                            "de.fraunhofer.aisec.glue.pipeline.contract" to contractId,
                            "de.fraunhofer.aisec.glue.pipeline.target" to targetId,
                        ),
                ) to c
            }

        return mapParallel(linkRequests, Network::remove) { _, item ->
            val (request, containers) = item
            val network = dockerClient.networks.create(request)
            try {
                containers.forEach { network.connect(it) }
                // We always want to have the controller reachable
                selfContainer?.let { network.connect(it) }
                return@mapParallel network
            } catch (e: Throwable) {
                network.remove()
                throw e
            }
        }
    }

    override suspend fun createPipeline(
        id: String,
        pipelineSpec: List<AugmentedGlueContainer>,
        contractId: String,
        targetId: String,
    ): ContainerPipeline {
        val containerCreateReqs = convertPipeline(id, pipelineSpec, contractId, targetId)

        if (containerCreateReqs.isEmpty()) {
            throw IllegalArgumentException("The provided spec contains no containers")
        }

        val sniDomain = connectorConfig.publicDomain
        val sniMode = sniDomain != null

        if (!sniMode) {
            // Legacy mode: publish port 8080 on a random ephemeral host port
            val publishPorts = listOf("8080/tcp")
            containerCreateReqs[0] =
                containerCreateReqs[0].copy(
                    exposedPorts = publishPorts.associateWith { emptyMap<Any, Any>() },
                    hostConfig =
                        containerCreateReqs[0].hostConfig!!.copy(
                            portBindings =
                                publishPorts.associateWith {
                                    listOf(
                                        PortBinding(
                                            hostPort = "",
                                        ),
                                    )
                                },
                        ),
                )
        } else {
            // SNI mode: set the hostname of the entry container to the pipeline FQDN.
            // No port publishing — Envoy routes TLS connections via SNI.
            val fqdn = "$id.$sniDomain"
            containerCreateReqs[0] =
                containerCreateReqs[0].copy(
                    hostname = fqdn,
                )
        }

        val containers = createContainers(id, containerCreateReqs)
        val networks =
            try {
                linkContainers(id, containers, contractId, targetId)
            } catch (e: Exception) {
                containers.forEach { it.runCatching { remove() } }
                throw e
            }

        if (sniMode) {
            // Connect the entry container to the Envoy mux network with the FQDN as
            // a network alias so that Envoy (also on that network) can resolve it
            // via Docker's embedded DNS.
            val fqdn = "$id.$sniDomain"
            try {
                val envoyNet = dockerClient.networks.get(config.envoyNetworkName)
                envoyNet.connect(containers[0], aliases = listOf(fqdn))
            } catch (e: Exception) {
                LOG.atError {
                    message = "Failed to connect entry container to Envoy mux network"
                    cause = e
                    payload = mapOf("container" to containers[0].name, "network" to config.envoyNetworkName)
                }
                containers.forEach { it.runCatching { remove() } }
                networks.forEach { it.runCatching { remove() } }
                throw e
            }
        }

        return DockerContainerPipeline(id, contractId, targetId, objectMapper, containers, networks, config, sniDomain)
    }

    override suspend fun getConnectorIp(relativeTo: ContainerInfo): String {
        val container = dockerClient.containers.get(relativeTo.name).inspect()
        // If the container is in no network, then this will be null
        val containerNetworks =
            container.networkSettings?.networks
                ?: throw IllegalStateException("Container ${relativeTo.name} is in no networks")
        val address =
            if (selfContainer != null) {
                val selfInspect = selfContainer!!.inspect()
                // Note: This may be O(n^2), but containerNetworks has length at most 2, so 2 calls for every network
                val sharedNetworks = selfInspect.networkSettings?.networks?.filter { (k, _) -> k in containerNetworks }
                // Return the ip address of the self container in the shared network
                sharedNetworks?.values?.firstOrNull()?.ipAddress
            } else {
                // The docker network properties are unfortunately only initialized after the container has started.
                containerNetworks.keys.firstOrNull()?.let { network ->
                    dockerClient.networks
                        .get(network)
                        .inspect()
                        .ipam
                        ?.config
                        ?.get(0)
                        ?.gateway
                }
            }
        return address ?: run {
            LOG.atError {
                message = "No shared address found for container ${relativeTo.name}"
                payload =
                    mapOf(
                        "container" to container,
                        "containerNetworks" to containerNetworks,
                    )
            }
            throw IllegalStateException("No shared address exists with container ${relativeTo.name}")
        }
    }

    override suspend fun getPipelineFromIp(ipAddress: String): String? {
        // Unfortunately, due to the highly nullable (defaultable?) nature of Go, we have to do a bunch of null checks
        // on every property...
        return dockerClient.containers
            .list()
            .filter { c ->
                c.networkSettings
                    ?.networks
                    ?.values
                    ?.any { n -> n.ipAddress == ipAddress } ?: false
            }.firstNotNullOfOrNull { c -> c.labels?.get("de.fraunhofer.aisec.glue.pipeline.id") }
    }

    class DockerConfig(
        inner: ApplicationConfig,
    ) : Config(inner) {
        val publicHostname: String by withDefault(stringProp(), InetAddress.getLocalHost().hostAddress)
        val envoyNetworkName by withDefault(stringProp(), "envoy-mux")
        val sniProxyPort by withDefault(intProp(), 8444)
        val pipelineEntryPort by withDefault(intProp(), 8080)
    }

    companion object {
        private val LOG = KotlinLogging.logger {}
    }
}
