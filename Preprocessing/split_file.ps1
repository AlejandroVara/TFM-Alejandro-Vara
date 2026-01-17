# File paths
$inputFilePath = "C:\Users\aleja\Desktop\MSc Data Science\TFM\mimic-iii-clinical-database-1.4\CHARTEVENTS.csv\CHARTEVENTS.csv"
$outputDirectory = "C:\Users\aleja\Desktop\MSc Data Science\TFM\SplitFiles2"
$chunkSize = 0.9GB  # Size of each chunk in bytes

# Create output directory if it does not exist
if (-Not (Test-Path $outputDirectory)) {
    New-Item -ItemType Directory -Path $outputDirectory
}

# Open the input file
$inputStream = [System.IO.File]::OpenRead($inputFilePath)
$reader = New-Object System.IO.StreamReader($inputStream)

# Initialize buffer size for reading
$bufferSize = 8192  # 8KB buffer for reading
$buffer = New-Object byte[] $bufferSize

# Variables for chunking
$chunkCounter = 1
$bytesRead = 0
$outputFilePath = Join-Path $outputDirectory "CHARTEVENTS_chunk_$chunkCounter.csv"

# Create output file stream
$outputStream = [System.IO.File]::Create($outputFilePath)
$outputWriter = New-Object System.IO.StreamWriter($outputStream)
$headerWritten = $false

# Start reading and splitting
while (($bytesRead = $reader.BaseStream.Read($buffer, 0, $buffer.Length)) -gt 0) {
    $chunkContent = [System.Text.Encoding]::UTF8.GetString($buffer, 0, $bytesRead)
    
    # Write header only once for the first chunk
    if (-not $headerWritten) {
        $outputWriter.WriteLine($chunkContent.Split("`n")[0])  # Write header
        $headerWritten = $true
    }
    
    # Write the chunk data to the file
    $outputWriter.Write($chunkContent)
    
    # Check if the current chunk exceeds the desired chunk size
    if ($outputStream.Length -ge $chunkSize) {
        # Close the current chunk and create a new one
        $outputWriter.Close()
        $outputStream.Close()

        $chunkCounter++
        $outputFilePath = Join-Path $outputDirectory "CHARTEVENTS_chunk_$chunkCounter.csv"
        
        # Create a new output file stream
        $outputStream = [System.IO.File]::Create($outputFilePath)
        $outputWriter = New-Object System.IO.StreamWriter($outputStream)
    }
}

# Close the streams
$reader.Close()
$inputStream.Close()
$outputWriter.Close()
$outputStream.Close()

Write-Host "Splitting complete. Files saved in $outputDirectory"
