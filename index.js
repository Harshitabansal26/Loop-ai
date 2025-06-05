const express = require('express');
const { v4: uuidv4 } = require('uuid');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

// In-memory storage
const ingestionStore = new Map();
const processingQueue = [];
let isProcessing = false;

// Priority values for sorting
const PRIORITY_VALUES = {
  'HIGH': 0,
  'MEDIUM': 1,
  'LOW': 2
};

// POST /ingest endpoint
app.post('/ingest', (req, res) => {
  const { ids, priority = 'MEDIUM' } = req.body;
  
  if (!ids || !Array.isArray(ids)) {
    return res.status(400).json({ error: 'Invalid request: ids must be an array' });
  }
  
  const ingestionId = uuidv4();
  const batches = [];
  
  // Create batches of 3 IDs
  for (let i = 0; i < ids.length; i += 3) {
    const batchIds = ids.slice(i, i + 3);
    const batchId = uuidv4();
    
    const batch = {
      batch_id: batchId,
      ids: batchIds,
      status: 'yet_to_start',
      priority: priority,
      created_time: Date.now()
    };
    
    batches.push(batch);
    processingQueue.push(batch);
  }
  
  // Sort queue by priority and creation time
  processingQueue.sort((a, b) => {
    if (PRIORITY_VALUES[a.priority] !== PRIORITY_VALUES[b.priority]) {
      return PRIORITY_VALUES[a.priority] - PRIORITY_VALUES[b.priority];
    }
    return a.created_time - b.created_time;
  });
  
  // Store ingestion data
  ingestionStore.set(ingestionId, {
    ingestion_id: ingestionId,
    status: 'yet_to_start',
    batches: batches
  });
  
  // Start processing if not already running
  if (!isProcessing) {
    processBatches();
  }
  
  return res.status(200).json({ ingestion_id: ingestionId });
});

// GET /status/:ingestionId endpoint
app.get('/status/:ingestionId', (req, res) => {
  const { ingestionId } = req.params;
  
  if (!ingestionStore.has(ingestionId)) {
    return res.status(404).json({ error: 'Ingestion ID not found' });
  }
  
  const ingestionData = ingestionStore.get(ingestionId);
  return res.status(200).json(ingestionData);
});

// Mock external API call
async function fetchDataFromExternalAPI(id) {
  return new Promise(resolve => {
    setTimeout(() => {
      resolve({ id, data: 'processed' });
    }, 1000); // Simulate API delay
  });
}

// Process batches asynchronously with rate limiting
async function processBatches() {
  isProcessing = true;
  
  while (processingQueue.length > 0) {
    const batch = processingQueue.shift();
    
    // Update batch status to triggered
    batch.status = 'triggered';
    updateIngestionStatus(batch);
    
    // Process each ID in the batch
    const promises = batch.ids.map(id => fetchDataFromExternalAPI(id));
    await Promise.all(promises);
    
    // Update batch status to completed
    batch.status = 'completed';
    updateIngestionStatus(batch);
    
    // Rate limit: wait 5 seconds before processing next batch
    await new Promise(resolve => setTimeout(resolve, 5000));
  }
  
  isProcessing = false;
}

// Update ingestion status based on batch statuses
function updateIngestionStatus(updatedBatch) {
  for (const [ingestionId, ingestionData] of ingestionStore.entries()) {
    const batchIndex = ingestionData.batches.findIndex(batch => 
      batch.batch_id === updatedBatch.batch_id);
    
    if (batchIndex !== -1) {
      ingestionData.batches[batchIndex] = updatedBatch;
      
      // Update overall status
      const allCompleted = ingestionData.batches.every(batch => batch.status === 'completed');
      const anyTriggered = ingestionData.batches.some(batch => batch.status === 'triggered');
      
      if (allCompleted) {
        ingestionData.status = 'completed';
      } else if (anyTriggered) {
        ingestionData.status = 'triggered';
      } else {
        ingestionData.status = 'yet_to_start';
      }
      
      break;
    }
  }
}

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

module.exports = app; // For testing
