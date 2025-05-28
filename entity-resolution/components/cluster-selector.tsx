"use client"

import { useEffect, useState } from "react"
import { useEntityResolution } from "@/context/entity-resolution-context"
import { getClusters } from "@/utils/api-client"
import type { Cluster } from "@/types/entity-resolution"
import { Card, CardContent } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { CheckCircle, ChevronLeft, ChevronRight } from "lucide-react"
import { Progress } from "@/components/ui/progress"

export default function ClusterSelector() {
  const { selectedClusterId, setSelectedClusterId, reviewProgress, refreshTrigger } = useEntityResolution()

  const [clusters, setClusters] = useState<Cluster[]>([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [loading, setLoading] = useState(true)
  const limit = 10

  useEffect(() => {
    async function loadClusters() {
      setLoading(true)
      try {
        const { clusters, total } = await getClusters(page, limit)
        setClusters(clusters)
        setTotal(total)

        // Auto-select first cluster if none selected
        if (!selectedClusterId && clusters.length > 0) {
          setSelectedClusterId(clusters[0].id)
        }

        // Check if current cluster is completed and auto-advance if needed
        if (selectedClusterId && reviewProgress[selectedClusterId] === 100) {
          const currentIndex = clusters.findIndex((cluster: Cluster) => cluster.id === selectedClusterId)
          if (currentIndex >= 0 && currentIndex < clusters.length - 1) {
            // Move to next cluster
            const nextCluster = clusters[currentIndex + 1]
            setSelectedClusterId(nextCluster.id)
          } else if (currentIndex === clusters.length - 1 && page < Math.ceil(total / limit)) {
            // Move to next page
            setPage(page + 1)
          }
        }
      } catch (error) {
        console.error("Failed to load clusters:", error)
      } finally {
        setLoading(false)
      }
    }

    loadClusters()
  }, [page, selectedClusterId, setSelectedClusterId, refreshTrigger, reviewProgress])

  const handlePageChange = (newPage: number) => {
    if (newPage >= 1 && newPage <= Math.ceil(total / limit)) {
      setPage(newPage)
    }
  }

  const getCoherenceColor = (score: number | null) => {
    if (score === null) return "bg-gray-200"
    if (score < 0.4) return "bg-red-500"
    if (score < 0.7) return "bg-yellow-500"
    return "bg-green-500"
  }

  const getProgressForCluster = (clusterId: string) => {
    return reviewProgress[clusterId] || 0
  }

  return (
    <div className="space-y-4 h-full flex flex-col">
      {loading ? (
        <div className="flex justify-center items-center flex-grow">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-gray-900"></div>
        </div>
      ) : (
        <>
          <div className="space-y-3 flex-grow overflow-auto pr-1">
            {clusters.map((cluster) => (
              <Card
                key={cluster.id}
                className={`cursor-pointer transition-all hover:shadow-md ${
                  selectedClusterId === cluster.id ? "ring-2 ring-primary" : ""
                }`}
                onClick={() => setSelectedClusterId(cluster.id)}
              >
                <CardContent className="p-3">
                  <div className="flex justify-between items-start mb-1">
                    <div className="font-medium text-sm">{cluster.name || `Cluster ${cluster.id}`}</div>

                  </div>

                  <div className="grid grid-cols-2 gap-2 text-xs text-muted-foreground mb-1">
                    <div>Entities: {cluster.entity_count}</div>
                    <div>Groups: {cluster.group_count}</div>
                  </div>

                  <div className="flex items-center gap-2 mb-1">
                    <div className="text-xs">Coherence:</div>
                    <div className={`h-2 w-2 rounded-full ${getCoherenceColor(cluster.average_coherence_score)}`}></div>
                    <div className="text-xs">
                      {cluster.average_coherence_score !== null
                        ? (cluster.average_coherence_score * 100).toFixed(0) + "%"
                        : "N/A"}
                    </div>
                  </div>

                  <div className="mt-2">
                    <div className="flex justify-between text-xs mb-1">
                      <span>Review Progress</span>
                      <span>{getProgressForCluster(cluster.id)}%</span>
                    </div>
                    <Progress value={getProgressForCluster(cluster.id)} className="h-1" />
                  </div>

                  {getProgressForCluster(cluster.id) === 100 && (
                    <div className="flex items-center mt-1 text-green-600 text-xs">
                      <CheckCircle className="h-3 w-3 mr-1" />
                      Review Complete
                    </div>
                  )}
                </CardContent>
              </Card>
            ))}
          </div>

          <div className="flex justify-between items-center pt-2 mt-auto">
            <Button variant="outline" size="sm" onClick={() => handlePageChange(page - 1)} disabled={page === 1}>
              <ChevronLeft className="h-4 w-4 mr-1" />
              Prev
            </Button>
            <span className="text-xs text-muted-foreground">
              Page {page} of {Math.ceil(total / limit)}
            </span>
            <Button
              variant="outline"
              size="sm"
              onClick={() => handlePageChange(page + 1)}
              disabled={page >= Math.ceil(total / limit)}
            >
              Next
              <ChevronRight className="h-4 w-4 ml-1" />
            </Button>
          </div>
        </>
      )}
    </div>
  )
}
