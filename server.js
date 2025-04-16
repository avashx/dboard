const express = require('express');
const mongoose = require('mongoose');
const socketIo = require('socket.io');
const http = require('http');
const cors = require('cors');
const path = require('path');
const axios = require('axios');
const protobuf = require('protobufjs');
const fs = require('fs');
const csv = require('csv-parse');

const app = express();
const server = http.createServer(app);
const io = socketIo(server, {
    cors: {
        origin: '*',
        methods: ['GET', 'POST']
    }
});

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));

// MongoDB Connection
mongoose.connect('mongodb://localhost:27017/busTracker', {
    useNewUrlParser: true,
    useUnifiedTopology: true
}).then(() => {
    console.log('Connected to MongoDB');
}).catch(err => {
    console.error('MongoDB connection error:', err);
});

// Schemas
const busSchema = new mongoose.Schema({
    busNo: String,
    latitude: Number,
    longitude: Number,
    routeNo: String,
    routeName: String,
    checked: Boolean,
    stopsRemaining: Number,
    mileage: Number,
    ticketRevenue: Number,
    fineRevenue: Number,
    lastUpdated: Date,
    routeCompletions: { type: Number, default: 0 }
});

const busStopSchema = new mongoose.Schema({
    stop_id: String,
    name: String,
    latitude: Number,
    longitude: Number
});

const checkSchema = new mongoose.Schema({
    busNo: String,
    routeNo: String,
    nonTicketHolders: Number,
    fineCollected: Number,
    lastStop: String,
    timestamp: { type: Date, default: Date.now }
});

const attendanceSchema = new mongoose.Schema({
    busNo: String,
    routeNo: String,
    conductorId: String,
    conductorWaiver: String,
    timestamp: { type: Date, default: Date.now }
});

const Bus = mongoose.model('Bus', busSchema);
const BusStop = mongoose.model('BusStop', busStopSchema);
const Check = mongoose.model('Check', checkSchema);
const Attendance = mongoose.model('Attendance', attendanceSchema);

// GTFS-realtime Setup
const protoFile = 'gtfs-realtime.proto';
let FeedMessage;
try {
    const root = protobuf.loadSync(protoFile);
    FeedMessage = root.lookupType('transit_realtime.FeedMessage');
} catch (err) {
    console.error('Error loading protobuf file:', err);
    process.exit(1);
}

const gtfsUrl = 'https://otd.delhi.gov.in/api/realtime/VehiclePositions.pb?key=7pnJf5w6MCh0JWrdisnafk0YhnKfUqxx';
let busData = [];
let busStops = [];
let routeMap = new Map();
const clientZoomLevels = new Map();
const ZOOM_THRESHOLD = 14;

// Parse CSV data
const parseCSV = (csvString) => {
    return new Promise((resolve, reject) => {
        const data = [];
        csv.parse(csvString, { columns: true, skip_empty_lines: true })
            .on('data', (row) => data.push(row))
            .on('end', () => resolve(data))
            .on('error', (err) => reject(err));
    });
};

// Initialize bus stops from CSV
const initializeBusStops = async () => {
    try {
        const stopsCsvString = fs.readFileSync('data/stops.csv', 'utf8');
        const stops = await parseCSV(stopsCsvString);
        busStops = stops.map(row => ({
            stop_id: row.stop_id,
            name: row.stop_name || 'Unknown Stop',
            latitude: parseFloat(row.stop_lat),
            longitude: parseFloat(row.stop_lon)
        }));
        console.log(`Parsed ${busStops.length} bus stops from CSV`);

        // Save to MongoDB if collection is empty
        const count = await BusStop.countDocuments();
        if (count === 0) {
            await BusStop.insertMany(busStops);
            console.log('Initialized bus stops in MongoDB');
        } else {
            // Update existing stops
            for (const stop of busStops) {
                await BusStop.updateOne(
                    { stop_id: stop.stop_id },
                    { $set: { name: stop.name, latitude: stop.latitude, longitude: stop.longitude } },
                    { upsert: true }
                );
            }
            console.log('Updated bus stops in MongoDB');
        }
    } catch (err) {
        console.error('Error parsing stops.csv:', err);
    }
};

// Initialize routes from CSV
const initializeRoutes = async () => {
    try {
        const routeCsvString = fs.readFileSync('data/routename.csv', 'utf8');
        const routes = await parseCSV(routeCsvString);
        routes.forEach(row => routeMap.set(row.route_id, row.route_name));
        console.log(`Parsed ${routeMap.size} routes from routename.csv`);
    } catch (err) {
        console.error('Error parsing routename.csv:', err);
    }
};

// Calculate distance between coordinates
function calculateDistance(lat1, lon1, lat2, lon2) {
    const R = 6371; // Radius in km
    const dLat = (lat2 - lat1) * Math.PI / 180;
    const dLon = (lon2 - lon1) * Math.PI / 180;
    const a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
        Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
        Math.sin(dLon / 2) * Math.sin(dLon / 2);
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    return R * c;
}

// Fetch GTFS-realtime bus data
const fetchBusData = async () => {
    let retries = 3;
    while (retries) {
        try {
            const response = await axios.get(gtfsUrl, { responseType: 'arraybuffer' });
            const buffer = response.data;
            const message = FeedMessage.decode(new Uint8Array(buffer));
            const data = FeedMessage.toObject(message, { longs: String, enums: String, bytes: String });

            const newBusData = data.entity
                .filter(entity => entity.vehicle && entity.vehicle.position)
                .map(entity => {
                    const busNo = entity.vehicle.vehicle.id || `BUS${Math.floor(Math.random() * 10000)}`;
                    const routeId = entity.vehicle.trip?.routeId || 'UNKNOWN';
                    return {
                        busNo,
                        routeNo: routeId,
                        routeName: routeMap.get(routeId) || routeId,
                        latitude: entity.vehicle.position.latitude,
                        longitude: entity.vehicle.position.longitude,
                        checked: false,
                        stopsRemaining: 10,
                        mileage: 1.75,
                        ticketRevenue: 0,
                        fineRevenue: 0,
                        lastUpdated: new Date(),
                        routeCompletions: 0
                    };
                });

            // Merge with MongoDB data
            const dbBuses = await Bus.find();
            for (const bus of newBusData) {
                const dbBus = dbBuses.find(b => b.busNo === bus.busNo);
                if (dbBus) {
                    bus.checked = dbBus.checked;
                    bus.stopsRemaining = dbBus.stopsRemaining;
                    bus.mileage = dbBus.mileage;
                    bus.ticketRevenue = dbBus.ticketRevenue;
                    bus.fineRevenue = dbBus.fineRevenue;
                    bus.routeCompletions = dbBus.routeCompletions;
                }

                // Update stopsRemaining based on stop proximity
                const nearestStop = busStops.find(stop =>
                    calculateDistance(bus.latitude, bus.longitude, stop.latitude, stop.longitude) < 0.05
                );

                if (nearestStop && bus.checked) {
                    const existingCheck = await Check.findOne({ busNo: bus.busNo });
                    const lastStop = existingCheck ? existingCheck.lastStop : null;
                    if (lastStop !== nearestStop.name) {
                        bus.stopsRemaining = Math.max(0, bus.stopsRemaining - 1);
                        if (bus.stopsRemaining === 0) {
                            bus.checked = false;
                            bus.routeCompletions += 1;
                            console.log(`Bus ${bus.busNo} completed route ${bus.routeName}, completions: ${bus.routeCompletions}`);
                        }
                        await Check.updateOne(
                            { busNo: bus.busNo },
                            { $set: { lastStop: nearestStop.name, timestamp: new Date() } },
                            { upsert: true }
                        );
                        await Bus.updateOne(
                            { busNo: bus.busNo },
                            {
                                $set: {
                                    stopsRemaining: bus.stopsRemaining,
                                    checked: bus.checked,
                                    routeCompletions: bus.routeCompletions,
                                    lastUpdated: new Date()
                                }
                            },
                            { upsert: true }
                        );
                        console.log(`Bus ${bus.busNo} at ${nearestStop.name}, stopsRemaining: ${bus.stopsRemaining}`);
                    }
                }
            }

            busData = newBusData;
            console.log(`Fetched ${busData.length} buses`);

            // Emit to clients
            io.sockets.sockets.forEach((socket) => {
                const zoomLevel = clientZoomLevels.get(socket.id) || 0;
                const updateData = { buses: busData };
                if (zoomLevel >= ZOOM_THRESHOLD) {
                    updateData.busStops = busStops;
                }
                socket.emit('busUpdate', updateData);
            });

            // Persist to MongoDB
            for (const bus of busData) {
                await Bus.updateOne(
                    { busNo: bus.busNo },
                    {
                        $set: {
                            latitude: bus.latitude,
                            longitude: bus.longitude,
                            routeNo: bus.routeNo,
                            routeName: bus.routeName,
                            checked: bus.checked,
                            stopsRemaining: bus.stopsRemaining,
                            mileage: bus.mileage,
                            ticketRevenue: bus.ticketRevenue,
                            fineRevenue: bus.fineRevenue,
                            routeCompletions: bus.routeCompletions,
                            lastUpdated: bus.lastUpdated
                        }
                    },
                    { upsert: true }
                );
            }

            break;
        } catch (error) {
            console.error('Error fetching GTFS data:', error.message);
            retries--;
            if (retries === 0) {
                console.error('Max retries reached for GTFS fetch.');
                break;
            }
            await new Promise(resolve => setTimeout(resolve, 2000));
        }
    }
};

// Routes
app.get('/', async (req, res) => {
    try {
        const buses = await Bus.find();
        const busStops = await BusStop.find();
        res.render('index', { buses, busStops });
    } catch (err) {
        console.error('Error rendering index:', err);
        res.status(500).send('Server Error');
    }
});

app.get('/api/buses', async (req, res) => {
    try {
        const buses = await Bus.find();
        res.json(buses);
    } catch (err) {
        console.error('Error fetching buses:', err);
        res.status(500).json({ error: 'Server Error' });
    }
});

app.get('/api/busStops', async (req, res) => {
    try {
        const busStops = await BusStop.find();
        res.json(busStops);
    } catch (err) {
        console.error('Error fetching bus stops:', err);
        res.status(500).json({ error: 'Server Error' });
    }
});

app.get('/api/analytics', async (req, res) => {
    try {
        const buses = await Bus.find();
        const checks = await Check.find();
        const attendances = await Attendance.find();

        const revenuePerBus = buses.map(bus => ({
            busNo: bus.busNo,
            revenue: (bus.ticketRevenue || 0) + (bus.fineRevenue || 0)
        }));

        const revenuePerRoute = buses.reduce((acc, bus) => {
            const route = acc.find(r => r.routeName === bus.routeName);
            if (route) {
                route.revenue += (bus.ticketRevenue || 0) + (bus.fineRevenue || 0);
            } else {
                acc.push({
                    routeName: bus.routeName,
                    revenue: (bus.ticketRevenue || 0) + (bus.fineRevenue || 0)
                });
            }
            return acc;
        }, []);

        const dtcLoss = checks.map(check => ({
            date: check.timestamp.toISOString().split('T')[0],
            loss: check.nonTicketHolders * 50
        }));

        const mileagePerBus = buses.map(bus => ({
            busNo: bus.busNo,
            mileage: bus.mileage || 1.75
        }));

        const costPerBus = buses.map(bus => ({
            busNo: bus.busNo,
            cost: (bus.mileage ? (1000 / bus.mileage) : 0) + 500
        }));

        const defaultersPerBus = checks.reduce((acc, check) => {
            const bus = acc.find(b => b.busNo === check.busNo);
            const busData = buses.find(b => b.busNo === check.busNo);
            if (bus) {
                bus.nonTicketHolders += check.nonTicketHolders;
            } else {
                acc.push({
                    busNo: check.busNo,
                    nonTicketHolders: check.nonTicketHolders,
                    latitude: busData ? busData.latitude : 28.6139,
                    longitude: busData ? busData.longitude : 77.2090
                });
            }
            return acc;
        }, []);

        const routeCompletions = buses.map(bus => ({
            busNo: bus.busNo,
            routeName: bus.routeName,
            completions: bus.routeCompletions || 0
        }));

        const totalBuses = buses.length;
        const totalStaff = attendances.length;
        const totalFineCollected = checks.reduce((sum, check) => sum + (check.fineCollected || 0), 0);
        const totalRevenue = revenuePerBus.reduce((sum, bus) => sum + bus.revenue, 0);
        const breakdownBuses = buses.filter(bus => bus.mileage < 1).length;

        res.json({
            revenuePerBus,
            revenuePerRoute,
            dtcLoss,
            mileagePerBus,
            costPerBus,
            defaultersPerBus,
            routeCompletions,
            totalBuses,
            totalStaff,
            totalFineCollected,
            totalRevenue,
            breakdownBuses
        });
    } catch (err) {
        console.error('Error fetching analytics:', err);
        res.status(500).json({ error: 'Server Error' });
    }
});

app.get('/api/routeCompletions', async (req, res) => {
    try {
        const { busNo, routeName } = req.query;
        const bus = await Bus.findOne({ busNo, routeName });
        res.json({ completions: bus ? bus.routeCompletions : 0 });
    } catch (err) {
        console.error('Error fetching route completions:', err);
        res.status(500).json({ error: 'Server Error' });
    }
});

app.post('/api/recordAttendance', async (req, res) => {
    try {
        const { busNo, routeNo, conductorId, conductorWaiver } = req.body;
        if (!busNo || !routeNo || !conductorId || !conductorWaiver) {
            return res.status(400).json({ success: false, error: 'Missing required fields' });
        }
        const attendance = new Attendance({
            busNo,
            routeNo,
            conductorId,
            conductorWaiver
        });
        await attendance.save();
        res.json({ success: true });
    } catch (err) {
        console.error('Error recording attendance:', err);
        res.status(500).json({ success: false, error: 'Server Error' });
    }
});

app.post('/api/checkBus', async (req, res) => {
    try {
        const { busNo, routeNo, nonTicketHolders, fineCollected } = req.body;
        if (!busNo || !routeNo || nonTicketHolders === undefined || fineCollected === undefined) {
            return res.status(400).json({ success: false, error: 'Missing required fields' });
        }

        const check = new Check({
            busNo,
            routeNo,
            nonTicketHolders,
            fineCollected
        });
        await check.save();

        // Update bus in MongoDB
        await Bus.updateOne(
            { busNo, routeNo },
            {
                $set: {
                    checked: true,
                    fineRevenue: fineCollected || 0,
                    stopsRemaining: 10,
                    lastUpdated: new Date()
                }
            }
        );

        // Update in-memory busData
        const bus = busData.find(b => b.busNo === busNo);
        if (bus) {
            bus.checked = true;
            bus.fineRevenue = fineCollected || 0;
            bus.stopsRemaining = 10;
        }

        // Emit update to clients
        io.sockets.sockets.forEach((socket) => {
            const zoomLevel = clientZoomLevels.get(socket.id) || 0;
            const updateData = { buses: busData };
            if (zoomLevel >= ZOOM_THRESHOLD) {
                updateData.busStops = busStops;
            }
            socket.emit('busUpdate', updateData);
        });

        res.json({ success: true });
    } catch (err) {
        console.error('Error checking bus:', err);
        res.status(500).json({ success: false, error: 'Server Error' });
    }
});

// Socket.IO Connections
io.on('connection', (socket) => {
    console.log('Client connected:', socket.id);
    socket.on('zoomLevel', (zoom) => {
        clientZoomLevels.set(socket.id, zoom);
        const updateData = { buses: busData };
        if (zoom >= ZOOM_THRESHOLD) {
            updateData.busStops = busStops;
        }
        socket.emit('busUpdate', updateData);
    });
    socket.on('disconnect', () => {
        clientZoomLevels.delete(socket.id);
        console.log('Client disconnected:', socket.id);
    });
});

// Initialize and Start
const startServer = async () => {
    try {
        await initializeBusStops();
        await initializeRoutes();
        await fetchBusData();
        setInterval(fetchBusData, 5000); // Fetch every 5 seconds to balance performance

        const PORT = process.env.PORT || 3000;
        server.listen(PORT, () => {
            console.log(`Server running on port ${PORT}`);
        });
    } catch (err) {
        console.error('Failed to start server:', err);
        process.exit(1);
    }
};

startServer();

// Graceful shutdown
process.on('SIGTERM', async () => {
    console.log('SIGTERM received. Closing MongoDB connection...');
    await mongoose.connection.close();
    server.close(() => {
        console.log('Server closed.');
        process.exit(0);
    });
});

process.on('SIGINT', async () => {
    console.log('SIGINT received. Closing MongoDB connection...');
    await mongoose.connection.close();
    server.close(() => {
        console.log('Server closed.');
        process.exit(0);
    });
});