const express = require('express');
const redis = require('redis');
const cors = require('cors');
const bodyParser = require('body-parser');
const multer = require('multer');
const csv = require('csv-parser');
const fs = require('fs');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 5000;

// Middleware
app.use(cors());
app.use(bodyParser.json());

// Multer setup for file uploads
const upload = multer({ dest: 'uploads/' });

// Connect to Redis
const client = redis.createClient({
  url: 'redis://127.0.0.1:6379'
});

client.connect()
  .then(() => console.log('Connected to Redis'))
  .catch(err => console.error('Redis connection error:', err));

// CRUD Operations

// Route to save student data
app.post('/students', async (req, res) => {
  const { id, name, course, age, address, email, phone, gender } = req.body;

  if (!id || !name || !course || !age || !address || !email || !phone || !gender) {
    return res.status(400).json({ message: 'All fields are required' });
  }

  try {
    // Set student data in Redis (using object syntax for Redis v4 and above)
    const studentData = { name, course, age, address, email, phone, gender };
    // Save student data in Redis hash
    await client.hSet(`student:${id}`, 'name', studentData.name);
    await client.hSet(`student:${id}`, 'course', studentData.course);
    await client.hSet(`student:${id}`, 'age', studentData.age);
    await client.hSet(`student:${id}`, 'address', studentData.address);
    await client.hSet(`student:${id}`, 'email', studentData.email);
    await client.hSet(`student:${id}`, 'phone', studentData.phone);
    await client.hSet(`student:${id}`, 'gender', studentData.gender);

    // Respond with success message
    res.status(201).json({ message: 'Student saved successfully' });
  } catch (error) {
    console.error('Error saving student:', error);
    res.status(500).json({ message: 'Failed to save student' });
  }
});

// Route to upload CSV file and save data to Redis
app.post('/upload', upload.single('file'), (req, res) => {
  if (!req.file) {
    return res.status(400).json({ message: 'No file uploaded' });
  }

  const results = [];
  const expectedHeaders = ['id', 'name', 'course', 'age', 'address', 'email', 'phone', 'gender'];

  fs.createReadStream(req.file.path)
    .pipe(csv())
    .on('headers', (headers) => {
      const isValid = expectedHeaders.every((header, index) => header === headers[index]);
      if (!isValid) {
        throw new Error('Invalid CSV format');
      }
    })
    .on('data', (data) => results.push(data))
    .on('end', async () => {
      try {
        for (const student of results) {
          const { id, name, course, age, address, email, phone, gender } = student;

          if (!id || !name || !course || !age || !address || !email || !phone || !gender) {
            throw new Error('Invalid CSV format');
          }

          // Save student data in Redis hash
          await client.hSet(`student:${id}`, 'name', name);
          await client.hSet(`student:${id}`, 'course', course);
          await client.hSet(`student:${id}`, 'age', age);
          await client.hSet(`student:${id}`, 'address', address);
          await client.hSet(`student:${id}`, 'email', email);
          await client.hSet(`student:${id}`, 'phone', phone);
          await client.hSet(`student:${id}`, 'gender', gender);
        }

        res.status(201).json({ message: 'CSV data saved successfully' });
      } catch (error) {
        console.error('Error saving data:', error);
        res.status(500).json({ message: 'Error saving data' });
      } finally {
        // Remove the uploaded file
        fs.unlinkSync(req.file.path);
      }
    })
    .on('error', (error) => {
      console.error('CSV parsing error:', error);
      res.status(400).json({ message: 'Invalid CSV format' });
      fs.unlinkSync(req.file.path);
    });
});

// Read (R)
app.get('/students/:id', async (req, res) => {
  const id = req.params.id;
  const student = await client.hGetAll(`student:${id}`);
  if (Object.keys(student).length === 0) {
    return res.status(404).json({ message: 'Student not found' });
  }
  res.json(student);
});

// Read all students
app.get('/students', async (req, res) => {
  const keys = await client.keys('student:*');
  const students = await Promise.all(keys.map(async (key) => {
    return { id: key.split(':')[1], ...(await client.hGetAll(key)) };
  }));
  res.json(students);
});

// Get a student by ID
app.get('/students/:id', async (req, res) => {
  try {
    const id = req.params.id;
    const student = await client.hGetAll(`student:${id}`);

    if (Object.keys(student).length === 0) {
      return res.status(404).json({ message: 'Student not found' });
    }

    res.json(student);
  } catch (error) {
    console.error('Error fetching student:', error);
    res.status(500).json({ message: 'Failed to fetch student' });
  }
});

// Get all students (with search feature)
app.get('/students', async (req, res) => {
  try {
    const { search } = req.query;
    const keys = await client.keys('student:*');
    let students = await Promise.all(keys.map(async (key) => {
      return { id: key.split(':')[1], ...(await client.hGetAll(key)) };
    }));

    if (search) {
      const searchLower = search.toLowerCase();
      students = students.filter(student =>
        student.name.toLowerCase().includes(searchLower) ||
        student.course.toLowerCase().includes(searchLower)
      );
    }

    res.json(students);
  } catch (error) {
    console.error('Error fetching students:', error);
    res.status(500).json({ message: 'Failed to fetch students' });
  }
});

// Update student
app.put('/students/:id', async (req, res) => {
  const id = req.params.id;
  const { name, course, age, address, email, phone, gender } = req.body;

  if (!name && !course && !age && !address && !email && !phone && !gender) {
    return res.status(400).json({ message: 'At least one field is required to update' });
  }

  try {
    const existingStudent = await client.hGetAll(`student:${id}`);
    if (Object.keys(existingStudent).length === 0) {
      return res.status(404).json({ message: 'Student not found' });
    }

    // Update student data in Redis
    if (name) await client.hSet(`student:${id}`, 'name', name);
    if (course) await client.hSet(`student:${id}`, 'course', course);
    if (age) await client.hSet(`student:${id}`, 'age', age);
    if (address) await client.hSet(`student:${id}`, 'address', address);
    if (email) await client.hSet(`student:${id}`, 'email', email);
    if (phone) await client.hSet(`student:${id}`, 'phone', phone);
    if (gender) await client.hSet(`student:${id}`, 'gender', gender);

    res.status(200).json({ message: 'Student updated successfully' });
  } catch (error) {
    console.error('Error updating student:', error);
    res.status(500).json({ message: 'Failed to update student' });
  }
});

// Delete student
app.delete('/students/:id', async (req, res) => {
  try {
    const id = req.params.id;
    await client.del(`student:${id}`);
    res.status(200).json({ message: 'Student deleted successfully' });
  } catch (error) {
    console.error('Error deleting student:', error);
    res.status(500).json({ message: 'Failed to delete student' });
  }
});

// Start server
app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});