#!/usr/bin/env bb
(ns run-java
  (:require [babashka.process :as p]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.util.concurrent TimeUnit]))

(def tasks
  [{:name "forever-1" :cmd ["java" "Forever.java"]}
   {:name "forever-2" :cmd ["java" "Forever.java"]}
   {:name "forever-3" :cmd ["java" "Forever.java"]}])

(def timeout-secs 5)
(def restart-delay-ms 25000)

(def procs (atom {}))
(def shutting-down? (atom false))
(def lock (Object.))

;; One lock for all printing, so lines never get mixed together.
(def out-lock (Object.))

(defn log [& parts]
  (locking out-lock
    (println (str/join " " parts))
    (flush)))

;; Read a process stream line by line and print each line with a prefix.
(defn pipe-lines! [name stream]
  (future
    (with-open [r (io/reader stream)]
      (doseq [line (line-seq r)]
        (log (str "[" name "]") line)))))

(defn start! [{:keys [name cmd]}]
  (locking lock
    (when-not @shutting-down?
      (log "Starting" name)
      (try
        ;; No :inherit -> stdout/stderr are piped into bb
        (let [proc (apply p/process {} cmd)]
          (pipe-lines! name (:out proc))
          (pipe-lines! name (:err proc))
          (swap! procs assoc name proc)
          proc)
        (catch Exception e
          (log name "failed to start:" (ex-message e))
          :failed)))))

(defn supervise [{:keys [name] :as task}]
  (future
    (loop []
      (when-let [proc (start! task)]
        (when (not= proc :failed)
          (let [{:keys [exit]} @proc]
            (when-not @shutting-down?
              (log name "stopped (exit" (str exit ")")))))
        (when-not @shutting-down?
          (log name "restarting in" (quot restart-delay-ms 1000) "s")
          (Thread/sleep restart-delay-ms)
          (recur))))))

(defn stop! []
  (locking lock (reset! shutting-down? true))
  (log "\nShutting down" (count @procs) "process(es)...")
  (doseq [[_ proc] @procs]
    (p/destroy-tree proc))
  (doseq [[name proc] @procs
          :let [jp (:proc proc)]]
    (when-not (.waitFor jp timeout-secs TimeUnit/SECONDS)
      (log name "did not stop in time, killing it.")
      (.destroyForcibly jp)
      (.waitFor jp))
    (log name "exited with code" (.exitValue jp)))
  (log "Bye!"))

(.addShutdownHook (Runtime/getRuntime) (Thread. ^Runnable stop!))

(run! supervise tasks)
@(promise)
