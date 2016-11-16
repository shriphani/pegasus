(ns pegasus.dsl
  "DSL for easy extractors and stuff"
  (:require [net.cgrand.enlive-html :as html]
            [org.bovinegenius.exploding-fish :as uri]
            [pegasus.process :as process])
  (:import [java.io StringReader]))

(def default-extractor-options
  {:when identity
   :at-selector [:a]
   :follow :href
   :with-regex nil})

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

          merged-options-map (merge options-map
                                    default-extractor-options)

          enlive-sel  (:at-selector merged-options-map)
          
          follow-sel  (:follow merged-options-map)

          predicate (:when merged-options-map)
          
          tags
          (filter
           identity
           (html/select resource
                        enlive-sel))

          attrs
          (map
           (fn [a-tag]
             (get (:attrs a-tag) follow-sel))
           tags)

          all-extracted
          (filter
           identity
           (if (predicate obj)
             (map
              #(uri/resolve-uri url %)
              attrs)
             []))

          regex-filtered
          (if (:with-regex merged-options-map)
            (filter
             #(re-find (:with-regex merged-options-map)
                       %)
             all-extracted)
            all-extracted)]
      regex-filtered)))

(defn defextractors
  [& extractors]
  (reify process/PipelineComponentProtocol
    (initialize
      [this config]
      config)
    
    (run
      [this obj config]
      (let [extracted (distinct
                       (filter
                        identity
                        (flatten
                         (map
                          (fn [e]
                            (e obj))
                          extractors))))]
        
        (merge
         obj
         {:extracted extracted})))

    (clean
      [this config]
      nil)))
