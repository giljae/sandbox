;; Licensed to the Apache Software Foundation (ASF) under one
;; or more contributor license agreements.  See the NOTICE file
;; distributed with this work for additional information
;; regarding copyright ownership.  The ASF licenses this file
;; to you under the Apache License, Version 2.0 (the
;; "License"); you may not use this file except in compliance
;; with the License.  You may obtain a copy of the License at
;;
;; http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.

(ns org.apache.storm.daemon.drpc
  (:import [org.apache.storm.security.auth AuthUtils ThriftServer ThriftConnectionType ReqContext])
  (:import [org.apache.storm.security.auth.authorizer DRPCAuthorizerBase])
  (:import [org.apache.storm.generated DistributedRPC DistributedRPC$Iface DistributedRPC$Processor
            DRPCRequest DRPCExecutionException DistributedRPCInvocations DistributedRPCInvocations$Iface
            DistributedRPCInvocations$Processor])
  (:import [java.util.concurrent Semaphore ConcurrentLinkedQueue
            ThreadPoolExecutor ArrayBlockingQueue TimeUnit])
  (:import [org.apache.storm.daemon Shutdownable])
  (:import [java.net InetAddress])
  (:import [org.apache.storm.generated AuthorizationException]
           [org.apache.storm.utils VersionInfo])
  (:use [org.apache.storm config log util])
  (:use [org.apache.storm.daemon common])
  (:use [org.apache.storm.ui helpers])
  (:use compojure.core)
  (:use ring.middleware.reload)
  (:require [compojure.handler :as handler])
  (:require [metrics.meters :refer [defmeter mark!]])
  (:gen-class))

(defmeter drpc:num-execute-http-requests)
(defmeter drpc:num-execute-calls)
(defmeter drpc:num-result-calls)
(defmeter drpc:num-failRequest-calls)
(defmeter drpc:num-fetchRequest-calls)
(defmeter drpc:num-shutdown-calls)

(def STORM-VERSION (VersionInfo/getVersion))

(defn timeout-check-secs [] 5)

(defn acquire-queue [queues-atom function]
  (swap! queues-atom
    (fn [amap]
      (if-not (amap function)
        (assoc amap function (ConcurrentLinkedQueue.))
        amap)))
  (@queues-atom function))

(defn check-authorization
  ([aclHandler mapping operation context]
    (if (not-nil? context)
      (log-thrift-access (.requestID context) (.remoteAddress context) (.principal context) operation))
    (if aclHandler
      (let [context (or context (ReqContext/context))]
        (if-not (.permit aclHandler context operation mapping)
          (let [principal (.principal context)
                user (if principal (.getName principal) "unknown")]
              (throw (AuthorizationException.
                       (str "DRPC request '" operation "' for '"
                            user "' user is not authorized"))))))))
  ([aclHandler mapping operation]
    (check-authorization aclHandler mapping operation (ReqContext/context))))

;; TODO: change this to use TimeCacheMap
(defn service-handler [conf]
  (let [drpc-acl-handler (mk-authorization-handler (conf DRPC-AUTHORIZER) conf)
        ctr (atom 0)
        id->sem (atom {})
        id->result (atom {})
        id->start (atom {})
        id->function (atom {})
        id->request (atom {})
        request-queues (atom {})
        cleanup (fn [id] (swap! id->sem dissoc id)
                  (swap! id->result dissoc id)
                  (swap! id->function dissoc id)
                  (swap! id->request dissoc id)
                  (swap! id->start dissoc id))
        my-ip (.getHostAddress (InetAddress/getLocalHost))
        clear-thread (async-loop
                       (fn []
                         (doseq [[id start] @id->start]
                           (when (> (time-delta start) (conf DRPC-REQUEST-TIMEOUT-SECS))
                             (when-let [sem (@id->sem id)]
                               (.remove (acquire-queue request-queues (@id->function id)) (@id->request id))
                               (log-warn "Timeout DRPC request id: " id " start at " start)
                               (.release sem))
                             (cleanup id)))
                         (timeout-check-secs)))]
    (reify DistributedRPC$Iface
      (^String execute
        [this ^String function ^String args]
        (mark! drpc:num-execute-calls)
        (log-debug "Received DRPC request for " function " (" args ") at " (System/currentTimeMillis))
        (check-authorization drpc-acl-handler
                             {DRPCAuthorizerBase/FUNCTION_NAME function}
                             "execute")
        (let [id (str (swap! ctr (fn [v] (mod (inc v) 1000000000))))
              ^Semaphore sem (Semaphore. 0)
              req (DRPCRequest. args id)
              ^ConcurrentLinkedQueue queue (acquire-queue request-queues function)]
          (swap! id->start assoc id (current-time-secs))
          (swap! id->sem assoc id sem)
          (swap! id->function assoc id function)
          (swap! id->request assoc id req)
          (.add queue req)
          (log-debug "Waiting for DRPC result for " function " " args " at " (System/currentTimeMillis))
          (.acquire sem)
          (log-debug "Acquired DRPC result for " function " " args " at " (System/currentTimeMillis))
          (let [result (@id->result id)]
            (cleanup id)
            (log-debug "Returning DRPC result for " function " " args " at " (System/currentTimeMillis))
            (if (instance? DRPCExecutionException result)
              (throw result)
              (if (nil? result)
                (throw (DRPCExecutionException. "Request timed out"))
                result)))))

      DistributedRPCInvocations$Iface

      (^void result
        [this ^String id ^String result]
        (mark! drpc:num-result-calls)
        (when-let [func (@id->function id)]
          (check-authorization drpc-acl-handler
                               {DRPCAuthorizerBase/FUNCTION_NAME func}
                               "result")
          (let [^Semaphore sem (@id->sem id)]
            (log-debug "Received result " result " for " id " at " (System/currentTimeMillis))
            (when sem
              (swap! id->result assoc id result)
              (.release sem)
              ))))

      (^void failRequest
        [this ^String id]
        (mark! drpc:num-failRequest-calls)
        (when-let [func (@id->function id)]
          (check-authorization drpc-acl-handler
                               {DRPCAuthorizerBase/FUNCTION_NAME func}
                               "failRequest")
          (let [^Semaphore sem (@id->sem id)]
            (when sem
              (swap! id->result assoc id (DRPCExecutionException. "Request failed"))
              (.release sem)))))

      (^DRPCRequest fetchRequest
        [this ^String func]
        (mark! drpc:num-fetchRequest-calls)
        (check-authorization drpc-acl-handler
                             {DRPCAuthorizerBase/FUNCTION_NAME func}
                             "fetchRequest")
        (let [^ConcurrentLinkedQueue queue (acquire-queue request-queues func)
              ret (.poll queue)]
          (if ret
            (do (log-debug "Fetched request for " func " at " (System/currentTimeMillis))
              ret)
            (DRPCRequest. "" ""))))

      Shutdownable

      (shutdown
        [this]
        (mark! drpc:num-shutdown-calls)
        (.interrupt clear-thread)))))

(defn handle-request [handler]
  (fn [request]
    (handler request)))

(defn populate-context!
  "Populate the Storm RequestContext from an servlet-request. This should be called in each handler"
  [http-creds-handler servlet-request]
    (when http-creds-handler
      (.populateContext http-creds-handler (ReqContext/context) servlet-request)))

(defn webapp [handler http-creds-handler]
  (mark! drpc:num-execute-http-requests)
  (->
    (routes
      (POST "/drpc/:func" [:as {:keys [body servlet-request]} func & m]
        (let [args (slurp body)]
          (populate-context! http-creds-handler servlet-request)
          (.execute handler func args)))
      (POST "/drpc/:func/" [:as {:keys [body servlet-request]} func & m]
        (let [args (slurp body)]
          (populate-context! http-creds-handler servlet-request)
          (.execute handler func args)))
      (GET "/drpc/:func/:args" [:as {:keys [servlet-request]} func args & m]
          (populate-context! http-creds-handler servlet-request)
          (.execute handler func args))
      (GET "/drpc/:func/" [:as {:keys [servlet-request]} func & m]
          (populate-context! http-creds-handler servlet-request)
          (.execute handler func ""))
      (GET "/drpc/:func" [:as {:keys [servlet-request]} func & m]
          (populate-context! http-creds-handler servlet-request)
          (.execute handler func "")))
    (wrap-reload '[org.apache.storm.daemon.drpc])
    handle-request))

(defn launch-server!
  ([]
    (log-message "Starting drpc server for storm version '" STORM-VERSION "'")
    (let [conf (read-storm-config)
          worker-threads (int (conf DRPC-WORKER-THREADS))
          queue-size (int (conf DRPC-QUEUE-SIZE))
          drpc-http-port (int (conf DRPC-HTTP-PORT))
          drpc-port (int (conf DRPC-PORT))
          drpc-service-handler (service-handler conf)
          ;; requests and returns need to be on separate thread pools, since calls to
          ;; "execute" don't unblock until other thrift methods are called. So if
          ;; 64 threads are calling execute, the server won't accept the result
          ;; invocations that will unblock those threads
          handler-server (when (> drpc-port 0)
                           (ThriftServer. conf
                             (DistributedRPC$Processor. drpc-service-handler)
                             ThriftConnectionType/DRPC))
          invoke-server (ThriftServer. conf
                          (DistributedRPCInvocations$Processor. drpc-service-handler)
                          ThriftConnectionType/DRPC_INVOCATIONS)
          http-creds-handler (AuthUtils/GetDrpcHttpCredentialsPlugin conf)]
      (add-shutdown-hook-with-force-kill-in-1-sec (fn []
                                                    (if handler-server (.stop handler-server))
                                                    (.stop invoke-server)))
      (log-message "Starting Distributed RPC servers...")
      (future (.serve invoke-server))
      (when (> drpc-http-port 0)
        (let [app (-> (webapp drpc-service-handler http-creds-handler)
                    requests-middleware)
              filter-class (conf DRPC-HTTP-FILTER)
              filter-params (conf DRPC-HTTP-FILTER-PARAMS)
              filters-confs [{:filter-class filter-class
                              :filter-params filter-params}]
              https-port (int (conf DRPC-HTTPS-PORT))
              https-ks-path (conf DRPC-HTTPS-KEYSTORE-PATH)
              https-ks-password (conf DRPC-HTTPS-KEYSTORE-PASSWORD)
              https-ks-type (conf DRPC-HTTPS-KEYSTORE-TYPE)
              https-key-password (conf DRPC-HTTPS-KEY-PASSWORD)
              https-ts-path (conf DRPC-HTTPS-TRUSTSTORE-PATH)
              https-ts-password (conf DRPC-HTTPS-TRUSTSTORE-PASSWORD)
              https-ts-type (conf DRPC-HTTPS-TRUSTSTORE-TYPE)
              https-want-client-auth (conf DRPC-HTTPS-WANT-CLIENT-AUTH)
              https-need-client-auth (conf DRPC-HTTPS-NEED-CLIENT-AUTH)]

          (storm-run-jetty
           {:port drpc-http-port
            :configurator (fn [server]
                            (config-ssl server
                                        https-port
                                        https-ks-path
                                        https-ks-password
                                        https-ks-type
                                        https-key-password
                                        https-ts-path
                                        https-ts-password
                                        https-ts-type
                                        https-need-client-auth
                                        https-want-client-auth)
                            (config-filter server app filters-confs))})))
      (start-metrics-reporters conf)
      (when handler-server
        (.serve handler-server)))))

(defn -main []
  (setup-default-uncaught-exception-handler)
  (launch-server!))
