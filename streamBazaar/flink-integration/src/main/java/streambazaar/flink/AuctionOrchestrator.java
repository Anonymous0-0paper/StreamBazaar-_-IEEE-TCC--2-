package streambazaar.flink;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Orchestrates the StreamBazaar control loop:
 * 1. Collects bids from tenant workloads
 * 2. Computes SLA-aware pricing
 * 3. Executes auction (winner selection, clearing price)
 * 4. Allocates resources with weighted fairness
 * 5. Decides on migrations/preemptions
 * 6. Emits allocation decisions
 */
public class AuctionOrchestrator extends KeyedProcessFunction<String, StreamBazaarJob.TenantEvent, StreamBazaarJob.AllocationDecision> {
    private static final Logger LOG = LoggerFactory.getLogger(AuctionOrchestrator.class);
    
    private static final double UTILIZATION_WEIGHT = 0.7;
    private static final double SLA_WEIGHT = 0.6;
    private static final double QUEUE_WEIGHT = 0.25;
    private static final double BALANCE_WEIGHT = 0.35;
    private static final double PRICE_SMOOTHING = 0.7;
    
    // Tenant state maps (per-key context)
    private final Map<String, TenantState> tenantState = new ConcurrentHashMap<>();
    private final Map<String, Long> lastAllocationTime = new ConcurrentHashMap<>();
    private final Map<String, Long> lastMigrationTime = new ConcurrentHashMap<>();
    
    private static final long ALLOCATION_INTERVAL_MS = 2000; // Every 2 seconds
    private static final long MIGRATION_COOLDOWN_MS = 60000;  // 60 second cooldown between migrations
    
    @Override
    public void processElement(StreamBazaarJob.TenantEvent event, KeyedProcessFunction<String, StreamBazaarJob.TenantEvent, StreamBazaarJob.AllocationDecision>.Context ctx, Collector<StreamBazaarJob.AllocationDecision> out) throws Exception {
        String tenantId = event.tenantId;
        
        // Initialize or retrieve tenant state
        tenantState.putIfAbsent(tenantId, new TenantState());
        TenantState state = tenantState.get(tenantId);
        
        // Update tenant state with incoming event
        state.updateFromEvent(event);
        
        // Check if it's time to trigger auction (time-based or event-based threshold)
        long currentTime = System.currentTimeMillis();
        Long lastAlloc = lastAllocationTime.getOrDefault(tenantId, 0L);
        
        if (currentTime - lastAlloc >= ALLOCATION_INTERVAL_MS || state.queueBacklog > 100) {
            // Execute pricing
            double bidFloor = computeBidFloor(state);
            state.bidFloor = bidFloor;
            
            // Compute bid (simplified: tenant bids at their priority level)
            double bid = bidFloor * event.priority;
            state.currentBid = bid;
            
            // Execute allocation based on bid and SLA
            double allocatedResources = computeAllocation(state);
            
            // Check for migration
            boolean shouldMigrate = checkMigration(state, currentTime);
            
            // Create and emit allocation decision
            StreamBazaarJob.AllocationDecision decision = new StreamBazaarJob.AllocationDecision();
            decision.tenantId = tenantId;
            decision.timestamp = currentTime;
            decision.allocatedResources = allocatedResources;
            decision.cpuShare = allocatedResources * 0.6;  // 60% CPU, 40% memory
            decision.memoryShare = allocatedResources * 0.4;
            decision.preempted = shouldMigrate;
            
            out.collect(decision);
            
            // Update timing
            lastAllocationTime.put(tenantId, currentTime);
            
            LOG.info("Allocation for tenant {}: bid={}, allocated={}, preempted={}", 
                    tenantId, String.format("%.2f", bid), 
                    String.format("%.2f", allocatedResources), shouldMigrate);
        }
    }
    
    /**
     * Compute SLA-aware bid floor pricing with smoothing
     */
    private double computeBidFloor(TenantState state) {
        // Base pricing on utilization, SLA pressure, queue backlog, credit balance
        double utilizationPrice = state.currentUtilization * UTILIZATION_WEIGHT;
        double slaPrice = Math.max(0, 1.0 - (state.p99Latency / state.slaTarget)) * SLA_WEIGHT;
        double queuePrice = Math.min(1.0, state.queueBacklog / 100.0) * QUEUE_WEIGHT;
        double balancePrice = Math.max(0, (state.creditBalance - 50.0) / 100.0) * BALANCE_WEIGHT;
        
        double rawPrice = utilizationPrice + slaPrice + queuePrice + balancePrice;
        
        // Apply temporal smoothing
        if (state.lastBidFloor > 0) {
            rawPrice = PRICE_SMOOTHING * state.lastBidFloor + (1.0 - PRICE_SMOOTHING) * rawPrice;
        }
        
        state.lastBidFloor = rawPrice;
        return Math.max(0.1, rawPrice); // Floor at 0.1 to avoid zero bids
    }
    
    /**
     * Compute weighted fair allocation based on SLA urgency and credit balance
     */
    private double computeAllocation(TenantState state) {
        // Base allocation on effective weight
        double effectiveWeight = state.priority * (1.0 + Math.max(0, (state.slaTarget - state.p99Latency) / 1000.0));
        
        // Apply credit multiplier (tenants with more credits get proportionally more)
        effectiveWeight *= Math.max(0.5, Math.min(1.5, state.creditBalance / 75.0));
        
        // Water-filling: allocate more to tenants further from SLA
        double slaGap = Math.max(0, state.slaTarget - state.p99Latency);
        double waterFillBoost = slaGap / state.slaTarget;
        
        double baseAllocation = 0.5; // Baseline per tenant
        double allocation = baseAllocation * effectiveWeight * (1.0 + waterFillBoost);
        
        return Math.min(2.0, allocation); // Cap at 2.0
    }
    
    /**
     * Determine if migration/preemption is needed
     */
    private boolean checkMigration(TenantState state, long currentTime) {
        long lastMigration = lastMigrationTime.getOrDefault(state.id, 0L);
        
        // Check cooldown
        if (currentTime - lastMigration < MIGRATION_COOLDOWN_MS) {
            return false;
        }
        
        // Migration triggers:
        // 1. SLA breach rate exceeds 10%
        // 2. Estimated latency gap > 100ms beyond SLA target
        
        boolean slaBreachTrigger = state.slaBreachCount > state.processedCount * 0.1;
        boolean latencyGapTrigger = (state.p99Latency - state.slaTarget) > 100;
        
        if (slaBreachTrigger || latencyGapTrigger) {
            lastMigrationTime.put(state.id, currentTime);
            return true;
        }
        
        return false;
    }
    
    /**
     * Tenant state tracker
     */
    private static class TenantState {
        String id;
        double currentUtilization = 0.5;
        long p99Latency = 100;
        long slaTarget = 200;
        double priority = 1.0;
        int queueBacklog = 0;
        double creditBalance = 100.0;
        double bidFloor = 0.5;
        double currentBid = 0.75;
        double lastBidFloor = 0;
        long processedCount = 0;
        long slaBreachCount = 0;
        long lastEventTime = 0;
        
        void updateFromEvent(StreamBazaarJob.TenantEvent event) {
            this.id = event.tenantId;
            this.priority = event.priority;
            this.slaTarget = event.slaTarget;
            this.lastEventTime = event.timestamp;
            
            // Simulate queue growth with each event
            this.queueBacklog = Math.min(1000, this.queueBacklog + 1);
            this.processedCount++;
            
            // Update p99 based on simulated latency
            long eventLatency = (long)(Math.random() * 500);
            if (eventLatency > this.slaTarget) {
                this.slaBreachCount++;
            }
            this.p99Latency = Math.max(this.p99Latency, eventLatency);
            
            // Decay queue and credit balance
            if (this.processedCount % 10 == 0) {
                this.queueBacklog = Math.max(0, this.queueBacklog - 5);
                this.creditBalance = Math.min(200.0, this.creditBalance + 10.0);
            }
        }
    }
}
