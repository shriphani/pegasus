(ns pegasus.dsl
  "DSL for easy extractors and stuff"
  (:require [net.cgrand.enlive-html :as html]
            [org.bovinegenius.exploding-fish :as uri]
            [pegasus.process :as process])
  (:import [java.io StringReader]))

(defn extract
  "Constructs extractors."
  [& options]
  (fn [obj]
    (let [url  (:url obj)
          body (:body obj)
          
          resource (-> body
                       (StringReader.)
                       html/html-resource)

          options-map (into
                       {}
                       (map
                        vec
                        (partition 2 options)))

          enlive-sel  (:at-selector options-map)
          
          follow-sel  (:follow options-map)
          
          tags
          (filter
           identity
           (html/select resource
                        enlive-sel))

          attrs
          (map
           (fn [a-tag]
             (get (:attrs a-tag) follow-sel))
           tags)]

      (map
       #(uri/resolve-uri url %)
       attrs))))

(defn defextractors
  [& extractors]
  (reify process/PipelineComponentProtocol
    (initialize
      [this config]
      config)
    
    (run
      [this obj config]
      (let [extracted (filter
                       identity
                       (flatten
                        (map
                         (fn [e]
                           (e obj))
                         extractors)))]
        (println extracted)
        (merge
         obj
         {:extracted extracted})))

    (clean
      [this config]
      nil)))
